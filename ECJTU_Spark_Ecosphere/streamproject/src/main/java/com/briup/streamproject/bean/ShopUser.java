package com.briup.streamproject.bean;


public class ShopUser {
    private Long id;
    private String  loginName;
    private String password;
    private String  realName;
    private String demo;

    public ShopUser() {

    }

    public ShopUser(String loginName, String password, String realName) {
        this.id = id;
        this.loginName = loginName;
        this.password = password;
        this.realName = realName;
    }

    public ShopUser(String loginName, String password, String realName, String demo) {
        this.id = id;
        this.loginName = loginName;
        this.password = password;
        this.realName = realName;
        this.demo = demo;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }


    public String getDemo() {
        return demo;
    }

    public void setDemo(String demo) {
        this.demo = demo;
    }

    public String getRealName() {

        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    public String getPassword() {

        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }


    @Override
    public String toString() {
        return "ShopUser{" +
                "id=" + id +
                ", loginName='" + loginName + '\'' +
                ", password='" + password + '\'' +
                ", realName='" + realName + '\'' +
                ", demo='" + demo + '\'' +
                '}';
    }
}
