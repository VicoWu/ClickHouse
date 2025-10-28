#pragma once

namespace DB
{

struct SerializationInfoSettings
{
    const double ratio_of_defaults_for_sparse = 1.0;
    const bool choose_kind = false;
    // 如果“用于选择稀疏序列化的默认值占比阈值”被设置为≥1.0，则视作“总是采用默认（不做按占比进行自适应使用稀疏）”，
    // 所以，这种情况下无需统计各 part 的默认值比例，也不会因为默认值很多而自动切换为稀疏序列化
    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0; }
};

}
