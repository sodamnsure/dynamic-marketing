# dynamic-marketing

## Maven项目命名规范
groupId:定义当前Maven项目隶属的实际项目，例如org.sonatype.nexus，此id前半部分org.sonatype代表此项目隶属的组织或公司，后部分代表项目的名称，如果此项目多模块话开发的话就子模块可以分为org.sonatype.nexus.plugins和org.sonatype.nexus.utils等。  
特别注意的是groupId不应该对应项目隶属的组织或公司，也就是说groupId不能只有org.sonatype而没有nexus。  
artifactId是构件ID，该元素定义实际项目中的一个Maven项目或者是子模块，如上面官方约定中所说，构建名称必须小写字母，没有其他的特殊字符，推荐使用“实际项目名称－模块名称”的方式定义，例如：spirng-mvn、spring-core等。