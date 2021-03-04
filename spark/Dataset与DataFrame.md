# Dataset 与 DataFrame的比较

* Dataset 
      
	  类型化的，在编译时就会检查类型是否符合规范，仅适用于基于Java虚拟机的语言（比如Scala和Java），并通过case类或者Java beans指定类型
* DataFrame
      
	  非类型化的，Spark完全负责维护它们的类型，仅在运行时检查这些类型是否与schema中指定的类型一致