## 抽象类

##### 抽象类不能创建实例，但是可以包含抽象方法

```java
public abstract class GeometricObject {
    private String Color = "white";
    private boolean filled;
    public String getColor() {
        return color;
    }
    public abstract double getArea();
    public abstract double getPerimeter();
}

public class Circle extends GeometricObject() {
    //class defination
}
```



## 接口

##### 接口只包含常量和抽象方法，是一种特殊的类。

##### Java中，一个类只能继承一个父类，如果想继承多个父类则需要使用接口

##### 类实现接口时，必须实现接口所有的方法，否则将被定义为抽象类

```java
//defination: [修饰符] interface [接口名]
public interface Animal {
    public static final int k = 1;
    public abstract void eat();
    public abstract void travel();
}

//接口的实现
public class MammalInt implements Animal {
    public void eat() {
        //...
    }
    public void travel() {
        //...s
    }
    public static void main(String args[]) {
        MammalInt m = new MammalInt();
        m.eat();
        m.travel();
    }
}
```

##### 接口继承：

```java
public class NewClass extends BaseClass 
			implements Interface1,...,InterfaceN {
    ...
}
```

