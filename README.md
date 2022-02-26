# 人机鼠标轨迹识别
## 模块分类
* [数据处理模块](https://github.com/XiaoTang233/mouse-track-recognition)
* [特征提取模块](https://github.com/XiaoTang233/mouse-track-recognition)
* [模型构建模块](https://github.com/XiaoTang233/mouse-track-recognition)
* [测试模块](https://github.com/XiaoTang233/mouse-track-recognition)

## 功能描述
* 本系统可以对输入系统的大量鼠标轨迹采样信息进行分析，判断该轨迹是人为的还是机器生成的。主要可以应用在网站的登录等的用户身份验证功能上，以此阻挡一部分非真人的操作，保障网站的信息安全。

* 功能的实现主要依靠`Hadoop`以及`Spark`开源框架提供的大数据的分布式储存和处理工具来实现的，其能够储存的数据量大并且数据操作的效率高，读写速度快。

* 数据集一共提取了22个特征信息，分别包括速度相关、时间相关、路程和位移相关、采样点相关的一些特征信息，丰富的特征信息使得建立的模型判断准确率比较高。
