# **wenziyue-redis-starter**

一个基于 Spring Boot 和 FastJSON 封装的 Redis Starter，提供了常用的 Redis 操作工具类 RedisUtils，简化在项目中对 Redis 的使用。

---

## **特色功能**

- 基于 Spring Boot 自动配置
- 默认使用 Lettuce 客户端（支持连接池）
- 支持 String、Hash、List、Set、ZSet 五大数据结构操作
- 支持 FastJSON 序列化存储对象
- 提供 Redis 工具类 RedisUtils，便于业务中直接调用
---

## **引入依赖**
首先在settings.xml中添加以下认证信息
```xml
<server>
    <id>wenziyue-redis</id>
    <username>你的GitHub用户名</username>
    <password>你的GitHub Token（建议只赋予 read:packages 权限）</password>
</server>
```

再在 `pom.xml` 中添加 GitHub 仓库地址：

```xml
<!-- pom.xml 中添加仓库地址（id 要与上面保持一致） -->
<repositories>
    <repository>
        <id>wenziyue-redis</id>
        <url>https://maven.pkg.github.com/wenziyue1984/wenziyue-redis-starter</url>
    </repository>
</repositories>
```
然后引入依赖：

```xml
<dependency>
    <groupId>com.wenziyue</groupId>
    <artifactId>wenziyue-redis-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```
> 💡 注意：你需要在 Maven 的 `settings.xml` 中配置 GitHub Token 授权，才能访问私有或 GitHub Packages 的依赖。
---

## **配置示例（application.yml）**

```yml
spring:
  redis:
    host: localhost
    port: 6379
    database: 0
    password: 
    timeout: 5000

    lettuce:
      pool:
        max-active: 20
        max-idle: 10
        min-idle: 2
        max-wait: 5000ms
```
---

## **使用示例**

### ***注入 RedisUtils***

```java
@Autowired
private RedisUtils redisUtils;
```

### ***String 操作***

```java
redisUtils.set("key", "hello");
Object val = redisUtils.get("key");
String str = val != null ? val.toString() : null;
redisUtils.delete("key");
```

### ***设置过期时间***

```java
redisUtils.set("key", "value", 60, TimeUnit.SECONDS); // 60 秒过期
Long expire = redisUtils.getExpire("key"); // 获取过期时间，0表示无该key，-1表示永久有效
```

### ***Hash 操作***

```java
redisUtils.hSet("hashKey", "field", "value");
Object val = redisUtils.hGet("hashKey", "field");
redisUtils.hDel("hashKey", "field");
```

### ***List 操作***

```java
redisUtils.lPush("listKey", "a");
redisUtils.rPush("listKey", "b");
Object left = redisUtils.lPop("listKey");
Object right = redisUtils.rPop("listKey");
```

### ***Set 操作***

```java
redisUtils.sAdd("setKey", "a", "b");
Set<Object> members = redisUtils.sMembers("setKey");
redisUtils.sRemove("setKey", "a");
```

### ***ZSet 操作***

```java
redisUtils.zAdd("zsetKey", "a", 1);
Set<Object> range = redisUtils.zRange("zsetKey", 0, -1);
redisUtils.zRemove("zsetKey", "a");
```

### ***FastJSON 序列化对象***

```java
User user = new User("张三", 18);
redisUtils.set("user:1", user);
User result = redisUtils.get("user:1", User.class);
```
---

## **注意事项**

- 默认使用 Lettuce 作为连接池客户端（推荐）
- 推荐为缓存数据设置合理的过期时间
- 对象存取使用 FastJSON 序列化
- RedisUtils 已对常见空值和异常情况做处理，开箱即用