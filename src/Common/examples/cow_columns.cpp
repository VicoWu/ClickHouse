#include <Common/COW.h>
#include <iostream>
#include <base/defines.h>


class IColumn : public COW<IColumn>
{
private:
    friend class COW<IColumn>;
    virtual MutablePtr clone() const = 0;

public:
    IColumn() = default;
    IColumn(const IColumn &) = default;
    virtual ~IColumn() = default;

    virtual int get() const = 0;
    virtual void set(int value) = 0;
};

using ColumnPtr = IColumn::Ptr; // 其实是 COW::immutable_ptr<Derived>
using MutableColumnPtr = IColumn::MutablePtr; // COW::mutable_ptr<Derived>

/**
 *  ConcreteColumn
 *     -> COWHelper<IColumn, ConcreteColumn> // Base: IColumn, Derived: ConcreteColumn
 *        -> IColumn
 *            -> COW<IColumn>  // Derived: IColumn
 *               -> boost::intrusive_ref_counter<Derived> // Derived: IColumn
 */
class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
{
private:
    friend class COWHelper<IColumn, ConcreteColumn>; // 友元函数，让基类可以访问ConcreteColumn的私有成员

    int data;
    explicit ConcreteColumn(int data_) : data(data_) {}
    ConcreteColumn(const ConcreteColumn &) = default;

public:
    int get() const override { return data; }
    void set(int value) override { data = value; }
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
    /**
     * 该代码通过ConcreteColumn在演示一个完整故事：
        -  ColumnPtr（只读共享）可以被拷贝共享
        - 当你要修改时，必须 mutate()：
        - 若共享（refcount>1）→ clone 出一份新对象（COW）
        - 若独占（refcount==1）→ 直接返回可写句柄（不复制）
        - 修改完成后，可把 MutablePtr move 给 Ptr，回到不可变共享态
     */
    // 其实就是 COWHelper<IColumn, ConcreteColumn>::create(1);， 因为ConcreteColumn继承了COWHelper的静态成员函数 create
    ColumnPtr x = ConcreteColumn::create(1); // 返回一个 IColumn::Ptr
    ColumnPtr y = x; // y = x：增加引用计数
    print(x, y);
    /**
     * x 指向 A，值 1，refcount 2
     * y 指向 A，值 1，refcount 2
     */
    chassert(x->get() == 1 && y->get() == 1);
    chassert(x->use_count() == 2 && y->use_count() == 2);
    chassert(x.get() == y.get());

    {
        /**
         * 在 COW 里：
         *  若 use_count > 1：clone
         *  否则：直接 assumeMutable（不 clone）
         */
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

    x = ConcreteColumn::create(0);
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
