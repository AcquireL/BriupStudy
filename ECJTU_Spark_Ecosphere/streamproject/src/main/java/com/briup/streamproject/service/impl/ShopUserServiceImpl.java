package com.briup.streamproject.service.impl;

import com.briup.streamproject.bean.ShopUser;
import com.briup.streamproject.mapper.ShopUserMapper;
import com.briup.streamproject.service.IShopUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service("ShopUserServiceImpl")
@Transactional
public class ShopUserServiceImpl  implements IShopUserService{

    @Autowired
    private ShopUserMapper shopUserMapper;

    @Override
    public void registerUser(ShopUser user) throws  Exception{
        if(user.getLoginName()==null&&user.getPassword()==null&&user.getRealName()==null){
            throw new Exception("用户信息不完整!");
        }
        ShopUser userByDao=shopUserMapper.selectUserByloginName(user);
        if(userByDao==null){
            shopUserMapper.saveUser(user);
        }else{
            throw new Exception("该用户名已经被或者占用了!");
        }
    }

    @Override
    public ShopUser loginUser(ShopUser user) throws  Exception{
        ShopUser userByDao=shopUserMapper.selectUserByloginName(user);
        if(userByDao==null){
            throw new Exception("用户不存在!");
        }else {
            if(userByDao.getPassword().equals(user.getPassword())){
                return userByDao;
            }else{
                //密码错误
               return  null;
            }
        }
    }

    @Override
    public ShopUser findUserByName(String userName)throws  Exception{
        ShopUser user=new ShopUser();
        user.setLoginName(userName);
        ShopUser userByDao=shopUserMapper.selectUserByloginName(user);
        if(userByDao==null){
            return null;
        }
        return userByDao;
    }
}
