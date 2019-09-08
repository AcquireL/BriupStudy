package com.briup.streamproject.mapper;

import com.briup.streamproject.bean.ShopUser;


public interface ShopUserMapper {

    /**
     * 保存用户信息
     * */
    void saveUser(ShopUser user);
    /**
     * 根据用户名查询用户信息
     * */
    ShopUser selectUserByloginName(ShopUser user);

}
