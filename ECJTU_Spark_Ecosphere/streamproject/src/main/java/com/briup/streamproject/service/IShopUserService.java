package com.briup.streamproject.service;

import com.briup.streamproject.bean.ShopUser;

public interface IShopUserService {
    /**
     * 用户注册
     * */
    void registerUser(ShopUser user) throws  Exception;
    /**
     * 用户登陆
     * */
    ShopUser loginUser(ShopUser user) throws  Exception;
    /**
     * 验证用户名唯一
     * */
    ShopUser findUserByName(String userName) throws  Exception;
}
