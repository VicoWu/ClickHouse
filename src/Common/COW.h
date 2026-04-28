#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <initializer_list>


/** Copy-on-write shared ptr.
  * Allows to work with shared immutable objects and sometimes unshare and mutate you own unique copy.
  *
  * Usage:

    class Column : public COW<Column>
    {
    private:
        friend class COW<Column>;

        /// Leave all constructors in private section. They will be available through 'create' method.
        Column();

        /// Provide 'clone' method. It can be virtual if you want polymorphic behaviour.
        virtual Column * clone() const;
    public:
        /// Correctly use const qualifiers in your interface.

        virtual ~Column() {}
    };

  * It will provide 'create' and 'mutate' methods.
  * And 'Ptr' and 'MutablePtr' types.
  * Ptr is refcounted pointer to immutable object.
  * MutablePtr is refcounted noncopyable pointer to mutable object.
  * MutablePtr can be assigned to Ptr through move assignment.
  *
  * 'create' method creates MutablePtr: you cannot share mutable objects.
  * To share, move-assign to immutable pointer.
  * 'mutate' method allows to create mutable noncopyable object from immutable object:
  *   either by cloning or by using directly, if it is not shared.
  * These methods are thread-safe.
  *
  * Example:
  *
    /// Creating and assigning to immutable ptr.
    Column::Ptr x = Column::create(1);
    /// Sharing single immutable object in two ptrs.
    Column::Ptr y = x;

    /// Now x and y are shared.

    /// Change value of x.
    {
        /// Creating mutable ptr. It can clone an object under the hood if it was shared.
        Column::MutablePtr mutate_x = IColumn::mutate(std::move(x));
        /// Using non-const methods of an object.
        mutate_x->set(2);
        /// Assigning pointer 'x' to mutated object.
        x = std::move(mutate_x);
    }

    /// Now x and y are unshared and have different values.

  * Note. You may have heard that COW is bad practice.
  * Actually it is, if your values are small or if copying is done implicitly.
  * This is the case for string implementations.
  *
  * In contrast, COW is intended for the cases when you need to share states of large objects,
  * (when you usually will use std::shared_ptr) but you also want precise control over modification
  * of this shared state.
  *
  * Caveats:
  * - after a call to 'mutate' method, you can still have a reference to immutable ptr somewhere.
  * - as 'mutable_ptr' should be unique, it's refcount is redundant - probably it would be better
  *   to use std::unique_ptr for it somehow.
  */
template <typename Derived>
class COW : public boost::intrusive_ref_counter<Derived>
{
private:
    /**
     * 这也是一个const重载(而不是函数名重载)
     * static_cast<Derived*>(this) 这种“指针向下转型”， 不会、也不可能调用 Derived 的构造函数，它只是在改 "你怎么看这个地址"
     * @return
     */
    Derived * derived() { return static_cast<Derived *>(this); }
    const Derived * derived() const { return static_cast<const Derived *>(this); }

    // 定义两个类模板 mutable_ptr 和 immutable_ptr
protected:
    template <typename T>
    class mutable_ptr : public boost::intrusive_ptr<T> /// NOLINT
    {
    private:
        using Base = boost::intrusive_ptr<T>;
        // COW是immutable_ptr的friend，因此，可以通过COW::immutable_ptr来调用构造函数
        template <typename> friend class COW;
        template <typename, typename> friend class COWHelper;

        // 显式构造函数，可以通过static_cast被调用
        explicit mutable_ptr(T * ptr) : Base(ptr) {}

    public:
        /// Copy: not possible.
        mutable_ptr(const mutable_ptr &) = delete; // 不允许有拷贝构造函数

        /// Move: ok.
        mutable_ptr(mutable_ptr &&) = default; ///  允许有移动构造函数
        mutable_ptr & operator=(mutable_ptr &&) = default; /// 允许移动赋值

        /// Initializing from temporary of compatible type.
        template <typename U>
        mutable_ptr(mutable_ptr<U> && other) : Base(std::move(other)) {} /// NOLINT

        mutable_ptr() = default;

        mutable_ptr(std::nullptr_t) {} /// NOLINT
    };

public:
    using MutablePtr = mutable_ptr<Derived>;

protected:
    // 子类可以使用immutable_ptr和mutable_ptr，比如，在COWHelper中可以使用
    template <typename T>
    class immutable_ptr : public boost::intrusive_ptr<const T> /// NOLINT
    {
    private:
        using Base = boost::intrusive_ptr<const T>;
        // COW是immutable_ptr的friend，因此，可以通过COW::immutable_ptr来调用构造函数
        template <typename> friend class COW;
        template <typename, typename> friend class COWHelper;
        // explicit构造函数，可以通过static_cast被调用
        explicit immutable_ptr(const T * ptr) : Base(ptr) {}

    public:
        /// Copy from immutable ptr: ok.
        immutable_ptr(const immutable_ptr &) = default; // 默认的赋值构造函数
        immutable_ptr & operator=(const immutable_ptr &) = default;

        template <typename U>
        immutable_ptr(const immutable_ptr<U> & other) : Base(other) {} /// NOLINT

        /// Move: ok.
        immutable_ptr(immutable_ptr &&) = default; /// 默认的移动构造函数
        immutable_ptr & operator=(immutable_ptr &&) = default; /// 移动赋值

        /// Initializing from temporary of compatible type.
        template <typename U>
        immutable_ptr(immutable_ptr<U> && other) : Base(std::move(other)) {} /// NOLINT

        /// Move from mutable ptr: ok.
        template <typename U>
        immutable_ptr(mutable_ptr<U> && other) : Base(std::move(other)) {} /// NOLINT

        /// Copy from mutable ptr: not possible.
        template <typename U>
        immutable_ptr(const mutable_ptr<U> &) = delete; // 不允许有从mutable_ptr到immutable_ptr的拷贝构造函数ctor

        immutable_ptr() = default;

        immutable_ptr(std::nullptr_t) {} /// NOLINT
    };

public:
    using Ptr = immutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args &&... args) { return MutablePtr(new Derived(std::forward<Args>(args)...)); }

    template <typename T>
    static MutablePtr create(std::initializer_list<T> && arg) { return create(std::forward<std::initializer_list<T>>(arg)); }
    /**
     * const / 非 const 成员函数重载，而不是函数名重载
     * 把derived()进行静态类型转换成 immutable_ptr<Derived>/ mutable_ptr<Derived>
     *
     * getPtr() 的 const / 非 const 重载并不是为了“返回不同类型”， 而是利用 constness 作为权限系统：
     *  const 对象只能获得共享只读指针，
     *  非 const 对象才能获得独占可写指针。
     *  通过 CRTP 的 derived() 和指针包装类型的 explicit 构造，
     *  ClickHouse 在类型系统层面强制了 COW 的不变式。

     * 编译器的调用规则是: 看调用点对象是不是 const
     *          const COW& x → 只能调用 f() const
     *          COW& x → 优先调用非 const 版本
     *
     * static_cast 不是“改内存”， 而是“请求一种合法的、编译期可决定的转换方式”。因此这里会调用Ptr/MutablePtr的构造函数
     * 由于COW是Ptr和MutablePtr的friend，因此可以在这里调用到Ptr和MutablePtr的构造函数
     *
     *
     * @return
     */
    Ptr getPtr() const { return static_cast<Ptr>(derived()); } // 这里调用 derived const(){}，返回 const Derived *
    MutablePtr getPtr() { return static_cast<MutablePtr>(derived()); } // 这里调用 derived const(){}，返回 Derived *

protected:
    // 返回一个 mutable_ptr<Derived>，这个方法是一个protect方法，意味着只有子类能调用，比如COWHelper
    // 这里的shallowMutate()不是进行修改的含义，而是准备进行修改，所以拷贝一份出来“供修改”
    // 区别于子类的shallowMutate
    MutablePtr shallowMutate() const
    {
        // 这个use_count定义在boost::intrusive_ref_counter中
        if (this->use_count() > 1)
            return derived()->clone(); // CRTP的特性：基类根本不需要在意派生类是否有clone()方法，派生类自己负责实现clone()方法，如果没实现但是又调用了shallowMutate()，编译期间报错
        else
            return assumeMutable();
    }

public:
    // COW::mutate
    // 可以看到，这里的mutate只是浅修改，是直接调用shallowMutate()
    // 子类的IColumn::mutate()会为mutate“深度拷贝”出一个副本
    static MutablePtr mutate(Ptr ptr)
    {
        return ptr->shallowMutate();
    }
    // COW::assumeMutable
    MutablePtr assumeMutable() const
    {
        /**
         * this 在 assumeMutable() const 里类型是：const COW*, const_cast<COW*>(this) 把它变成：COW*（去掉 const）
         * 所以，这里调用的为 非const版本的getPtr()，返回一个 MutablePtr (mutable_ptr<Derived>)
         */
        return const_cast<COW*>(this)->getPtr(); // 返回一个 MutablePtr (mutable_ptr<Derived>)
    }
    // COW::assumeMutableRef
    Derived & assumeMutableRef() const
    {
        return const_cast<Derived &>(*derived());
    }

protected:
    /// It works as immutable_ptr if it is const and as mutable_ptr if it is non const.
    template <typename T>
    class chameleon_ptr /// NOLINT
    {
        /**
         * 变色龙封装的是一个immutable_ptr，而不是一个mutable_ptr，因此， chameleon_ptr 本质上更像一个 “默认共享的只读句柄” ,
         * 但它提供非 const 的 get()/operator->/operator*，把我们带到可写的世界 ,
         * 换句话说：它把“是否可写”绑定到你拿到的是 const 还是非 const 的 chameleon_ptr。
         */
    private:
        immutable_ptr<T> value;

    public:
        template <typename... Args>
        /**
         * 把参数原样转发给value
         * 这就是“完美转发”（forwarding constructor），
         * 你给 chameleon_ptr 什么参数，它就原样转发给 value（内部 immutable_ptr）去构造
         * @tparam Args
         * @param args
         */
        chameleon_ptr(Args &&... args) : value(std::forward<Args>(args)...) {} /// NOLINT
        /**
         * 完美转发
         * @tparam U
         * @param arg
         */
        template <typename U>
        chameleon_ptr(std::initializer_list<U> && arg) : value(std::forward<std::initializer_list<U>>(arg)) {}

        // 如 const chameleon_ptr，就只能拿到只读子对象
        // value 的类型是 immutable_ptr<T>，本质上是 intrusive_ptr<const T>，因此只能拿到 const T*。
        const T * get() const { return value.get(); }

        // 非 const 版本：返回 T*
        // value 的类型是 immutable_ptr<T>， 这里调用的是  COW::assumeMutableRef,
        // 因为 immutable_ptr 重载了 operator -> (immutable_ptr继承了boost::intrusive_ptr，默认就重载了operator -> )，因此，这里其实是调用
        // 因此这里是 T -> assumeMutableRef()， 而在继承体系里面 T == Derived == IColumn / ColumnVector / ...
        // {
        //   const T* p =  value.operator->();
        //   p->assumeMutableRef();
        // }
        T * get() { return &value->assumeMutableRef(); }

        // 这里重载了操作符->
        const T * operator->() const { return get(); } // const版本,，这里会直接调用const T * get() const
        T * operator->() { return get(); }  // 非const版本，这里会调用 T * get()

        // 重载了操作符*
        const T & operator*() const { return *value; }
        // 获取一个可读写的T，这时候，已经通过调用COW::assumeMutableRef拷贝了一份出来，因此是可读写
        T & operator*() { return value->assumeMutableRef(); }

        // 重载了操作符&
        operator const immutable_ptr<T> & () const { return value; } /// NOLINT
        operator immutable_ptr<T> & () { return value; } /// NOLINT

        /// Get internal immutable ptr. Does not change internal use counter.
        /**
         * detach方法有一个右值修饰符 && ，这是一个编译期间检查机制，它规定这个 detach() 方法只能被右值对象调用。
         * detach 的意思是“剥离”。这个限定符在编译期强制要求：只有当你准备销毁或弃用当前的变色龙指针时（比如执行了 std::move(res->wrapped)），
         * 你才被允许调用它。这防止了开发者在不小心的情况下，把一个还在使用的指针给“掏空”了。
         * 注意，detach是非const函数，意味着只有非const的 chameleon_ptr 才能被detach
         * @return
         */
        immutable_ptr<T> detach() && { return std::move(value); }

        // 禁止隐式转换，必须显式转换，转换为bool的逻辑是: 是否为空指针
        explicit operator bool() const { return value != nullptr; }
        bool operator! () const { return value == nullptr; }

        bool operator== (const chameleon_ptr & rhs) const { return value == rhs.value; }
        bool operator!= (const chameleon_ptr & rhs) const { return value != rhs.value; }
    };

public:
    /** Use this type in class members for compositions.
      *
      * NOTE:
      * For classes with WrappedPtr members,
      * you must reimplement 'mutate' method, so it will call 'mutate' of all subobjects (do deep mutate).
      * It will guarantee, that mutable object have all subobjects unshared.
      *
      * NOTE:
      * If you override 'mutate' method in inherited classes, don't forget to make it virtual in base class or to make it call a virtual method.
      * (COW itself doesn't force any methods to be virtual).
      *
      * See example in "cow_compositions.cpp".
      */
    using WrappedPtr = chameleon_ptr<Derived>; // 这个WrappedPtr是public的
};


/** Helper class to support inheritance.
  * Example:
  *
  * class IColumn : public COW<IColumn>
  * {
  *     friend class COW<IColumn>;
  *     virtual MutablePtr clone() const = 0;
  *     virtual ~IColumn() {}
  * };
  *
  * class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
  * {
  *     friend class COWHelper<IColumn, ConcreteColumn>;
  * };
  *
  * Here is complete inheritance diagram:
  *
  * ConcreteColumn
  *  COWHelper<IColumn, ConcreteColumn>
  *   IColumn
  *    CowPtr<IColumn>
  *     boost::intrusive_ref_counter<IColumn>
  *
  * See example in "cow_columns.cpp".
  * // 所有的实体Column，比如ColumnString，ColumnVector<UInt32>都是直接继承了COWHelper
  */
template <typename Base, typename Derived>
class COWHelper : public Base
{
private:
    // 典型的CRTP用法，Derived调用derived()就可以获得对应的子类指针
    Derived * derived() { return static_cast<Derived *>(this); }
    const Derived * derived() const { return static_cast<const Derived *>(this); }

public:
    /**
     * 这里，Base只是一个模板参数，编译期间并不确定这个模板参数对应的具体类,因此前面必须添加typename告诉编译器: Base::immutable_ptr是一个类型，而不是一个变量
     * 模板不关心“你是谁”，只关心“你有没有我要的东西”。
     * 这里的代码这么写，必须要求Base中含有immutable_ptr和mutable_ptr，即遵循接口契约（interface contract）
     * 在当前的代码中，COW是满足要求的: 含有 immutable_ptr和mutable_ptr,因此如果编译的时候Base是COW，那么就可以编译通过
     * 由于编译器不知道immutable_ptr是一个类模板，因此，必须添加Base::template
     */
    using Ptr = typename Base::template immutable_ptr<Derived>; // immutable_ptr定义在COW中
    using MutablePtr = typename Base::template mutable_ptr<Derived>; // mutable_ptr定义在COW中
    // 直接给ColumnVector， ColumnString使用的静态方法，可以参考 cow_columns.cpp
    // 不是 virtual，也不需要对象实例；只是名字查找时，派生类作用域也能找到基类的静态成员
    template <typename... Args>
    static MutablePtr create(Args &&... args) { return MutablePtr(new Derived(std::forward<Args>(args)...)); }

    template <typename T>
    static MutablePtr create(std::initializer_list<T> && arg) { return MutablePtr(new Derived(std::forward<std::initializer_list<T>>(arg))); }
    /**
     * COWHelper::clone()，实际运行时继承了virtual IColumn::clone() = 0 方法
     * 写时拷贝, 由于Base是一个模板类，因此需要添加typename声明
     * 这里clone()是在Base(IColumn)里面定义的virtual函数，具体的clone() 实现这里放在COWHelper里面，因为COWHelper是从接口类(IColumn)到具体实现类(ColumnVector)
     * 的桥接类，所以，这里就对具体类进行拷贝构造
     * 在
     * @return
     */
    typename Base::MutablePtr clone() const override { return typename Base::MutablePtr(new Derived(*derived())); }

protected:
    // COWHelper中的shallowMutate()会调用父类COW的COW::shallowMutate()
    // 这里的Derived是 ConcreteColumn，
    // static_cast<Derived *>(Base::shallowMutate().get()) 是典型的CRTP风格(编译期多态)，调用者肯定是Derived类型
    // Base::shallowMutate() 返回的是 Base::MutablePtr，但我们需要的是 Derived::MutablePtr。所以 COWHelper 必须 把“基类指针包装”转换成“具体类指针包装”，于是就多了一层封装
    MutablePtr shallowMutate() const { return MutablePtr(static_cast<Derived *>(Base::shallowMutate().get())); }
};
