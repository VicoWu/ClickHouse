#include <Common/COW.h>
#include <iostream>
#include <base/defines.h>


class IColumn : public COW<IColumn>
{
private:
    friend class COW<IColumn>;

    virtual MutablePtr clone() const = 0;
    /**
     * 默认情况下，deepMutate()只是做shallowMutate就行，如果实在是有需要做深拷贝的，需要重写mutate()
     * 这里的deepMutate() 是virtual方法，因此会调用到子类ColumnComposition的deepMutate
     */

    virtual MutablePtr deepMutate() const { return shallowMutate(); }

public:
    IColumn() = default;
    IColumn(const IColumn &) = default;
    virtual ~IColumn() = default;

    virtual int get() const = 0;
    virtual void set(int value) = 0;
    /**
     * 输入一个immutable_ptr，返回一个mutable_ptr
     * @param ptr
     * @return
     */
    static MutablePtr mutate(Ptr ptr) { return ptr->deepMutate(); }
};

using ColumnPtr = IColumn::Ptr;
using MutableColumnPtr = IColumn::MutablePtr;

/**
 * ConcreteColumn -> COWHelper<IColumn, ConcreteColumn> -> IColumn -> COW<IColumn> -> boost::intrusive_ref_counter<Derived>
 */
class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
{
private:
    /**
     * template <typename Base, typename Derived>
     *  class COWHelper : public Base
     */
    friend class COWHelper<IColumn, ConcreteColumn>; // 允许COWHelper访问自己的私有函数

    int data;
    explicit ConcreteColumn(int data_) : data(data_) {} // 禁止隐式类型转换
    ConcreteColumn(const ConcreteColumn &) = default; // 默认的拷贝构造函数

public:
    int get() const override { return data; }
    void set(int value) override { data = value; }
};

/**
 * ColumnComposition -> COWHelper<IColumn, ConcreteColumn> -> IColumn -> COW<IColumn> -> boost::intrusive_ref_counter<IColumn>
 */
class ColumnComposition : public COWHelper<IColumn, ColumnComposition>
{
private:
    friend class COWHelper<IColumn, ColumnComposition>;

    /**
     * ColumnComposition的成员wrapped是一个变色龙指针  chameleon_ptr<Derived>
     * ConcreteColumn -> COWHelper<IColumn, ConcreteColumn> -> IColumn -> COW<IColumn> -> boost::intrusive_ref_counter<Derived>
     * 这里Derived=ConcreteColumn,因此，ConcreteColumn::WrappedPtr实际上是chameleon_ptr<ConcreteColumn>
    */
    ConcreteColumn::WrappedPtr wrapped; // WrappedPtr定义在COW中的public变量，

    // 调用 ConcreteColumn::create(data)，返回的就是一个MutablePtr<ConcreteColumn>,因为此时Derived就是 ConcreteColumn
    // 然后调用chameleon_ptr<Derived>的构造函数，这里Derived就是 ConcreteColumn, 因此实际上是调用
    // chameleon_ptr<ConcreteColumn>的构造函数

    explicit ColumnComposition(int data) : wrapped(ConcreteColumn::create(data)) {}
    ColumnComposition(const ColumnComposition &) = default;

    /**
     * 重写了 deepMutate()，相对于shallowMutate()
     * 这里的deepMutate() 是override的，因此一定是一个virtual function，这个virtual function定义在本文件的IColumn中
     */
    IColumn::MutablePtr deepMutate() const override
    {
        std::cerr << "Mutating\n";
        /**
         * shallowMutate定义在COWHelper中，此时，Derived的类型是ColumnComposition，
         * 因此返回了MutablePtr<ColumnComposition>
         */
        auto res = shallowMutate();
        /**
         * 由于res此时是MutablePtr，那么res->wrapped通过intrusive_ptr的运算符重载，
         *  也是一个非const的ColumnComposition，它的wrapped变量也是 非const的  chameleon_ptr<ConcreteColumn>
         * std::move(res->wrapped) 先把这个变色龙chameleon_ptr<ConcreteColumn> 变成一个弃用的xvalue，因为我们看到detach方法是一个右值方法，要求调用者必须是右值
         * 然后从中detach出对应的immutable_ptr<Derived>，这里的Derived也是 ConcreteColumn，因此 这里是返回了一个rvalue immutable_ptr<ConcreteColumn>
         */
        res->wrapped = IColumn::mutate(std::move(res->wrapped).detach());
        return res;
    }

public:
    /**
     * 这里是调用重载的Operator ->()，
     * 这个const重载的operator ->()根据wrapped的类型决定是调用const版本的还是非const版本的，
     * 调用const版本的则返回const T *，调用非const版本的则返回 T *
     */
    int get() const override { return wrapped->get(); }
    void set(int value) override { wrapped->set(value); }
};

template <typename ColPtr>
void print(const ColumnPtr & x, const ColPtr & y)
{
    std::cerr << "values:    " << x->get()        << ", " << y->get()       << "\n";
    std::cerr << "refcounts: " << x->use_count()  << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get()         << ", " << y.get()        << "\n";
}

int main(int, char **)
{
    ColumnPtr x = ColumnComposition::create(1);
    ColumnPtr y = x;
    print(x, y);
    chassert(x->get() == 1 && y->get() == 1);
    chassert(x->use_count() == 2 && y->use_count() == 2);
    chassert(x.get() == y.get());

    {
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        mut->set(2);
        print(x, mut);
        chassert(x->get() == 1 && mut->get() == 2);
        chassert(x->use_count() == 1 && mut->use_count() == 1);
        chassert(x.get() != mut.get());

        y = std::move(mut);
    }
    print(x, y);
    chassert(x->get() == 1 && y->get() == 2);
    chassert(x->use_count() == 1 && y->use_count() == 1);
    chassert(x.get() != y.get());

    x = ColumnComposition::create(0);
    print(x, y);
    chassert(x->get() == 0 && y->get() == 2);
    chassert(x->use_count() == 1 && y->use_count() == 1);
    chassert(x.get() != y.get());

    {
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        mut->set(3);
        print(x, mut);
        chassert(x->get() == 0 && mut->get() == 3);
        chassert(x->use_count() == 1 && mut->use_count() == 1);
        chassert(x.get() != mut.get());

        y = std::move(mut);
    }
    print(x, y);
    chassert(x->get() == 0 && y->get() == 3);
    chassert(x->use_count() == 1 && y->use_count() == 1);
    chassert(x.get() != y.get());

    return 0;
}
