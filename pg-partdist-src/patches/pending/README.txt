待接入补丁，不属于当前内核构建输入

0013-xlog-write-upto.patch：原样来自 mmp 4ce18da。
当前仅归档及静态检查，未应用到目标内核、未编译、不被扩展调用。
不要把此目录加入批量应用列表。N16 剩余适配说明见
../../docs/integration/new1/N16.txt。

真正启用时须完成 fa23 持久性/读取水位适配、重建内核并同步 pg-install。
