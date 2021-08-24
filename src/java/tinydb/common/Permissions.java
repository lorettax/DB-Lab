package tinydb.common;

/**
 * 表示对关系/文件的请求权限的类
 * 带有两个静态对象 READ_ONLY 和 READ_WRITE 的私有构造函数，代表两个级别的权限。
 */
public enum Permissions {
    READ_ONLY, READ_WRITE
}
