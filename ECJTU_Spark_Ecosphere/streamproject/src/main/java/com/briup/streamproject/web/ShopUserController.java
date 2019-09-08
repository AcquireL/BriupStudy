package com.briup.streamproject.web;

import com.briup.streamproject.bean.ShopUser;
import com.briup.streamproject.service.IShopUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Controller
@RequestMapping("/")
public class ShopUserController {

    @Autowired
    @Qualifier("ShopUserServiceImpl")
    private IShopUserService shopUserService;

    @RequestMapping("/registerUser")
    public void register(HttpServletRequest req, HttpServletResponse resp) throws  Exception{
        String loginName=req.getParameter("loginName");
        String password=req.getParameter("password");
        String realName=req.getParameter("realName");
        String demo=req.getParameter("demo");

        ShopUser user=new ShopUser(loginName,password,realName,demo);
        try {
            shopUserService.registerUser(user);
            resp.getWriter().println("注册成功");
        }catch(Exception e) {
            e.printStackTrace();
            resp.getWriter().println("注册失败");

        }
    }

    @RequestMapping("/loginUser")
    public void login(HttpServletRequest req, HttpServletResponse resp) throws  Exception{
        //    println("进入登陆模块！")
        String loginName=req.getParameter("loginName");
        String passwd=req.getParameter("password");
        if(loginName==""||passwd==""){
            resp.getWriter().println("请填写正确的信息");
        }else{
            try{
                ShopUser user=new ShopUser();
                user.setLoginName(loginName);
                user.setPassword(passwd);
                ShopUser info=shopUserService.loginUser(user);
                if(info!=null){
                    //将用户信息放在Session对象中
                    req.getSession().setAttribute("shopUser",info);
                    req.getSession().setAttribute("token",info.getLoginName());
                    //跳转到首页
                    resp.getWriter().println("登录成功");
                }else{
                    //密码错误
                    resp.getWriter().println("密码错误");
                }
            }catch(Exception e){
                e.printStackTrace();
                resp.getWriter().println("账户或密码错误");

            }
        }
    }
    @RequestMapping("/findUserByName")
    public void findUserByName(HttpServletRequest req, HttpServletResponse resp) throws  Exception{
        String loginName=req.getParameter("loginName");
        ShopUser user= shopUserService.findUserByName(loginName);
        String info="error";
        if(user==null){
            info="ok";
        }
        resp.getWriter().println(info);
    }


    @RequestMapping("/allWebSocket")
    @ResponseBody
    public void newAllWebSocket(@RequestParam String topic ) throws  Exception{
    }


    @RequestMapping("/logoutUser")
    public void logoutUser(HttpServletRequest req, HttpServletResponse resp) throws  Exception{
        //将用户信息从Session对象中移除
        req.getSession().removeAttribute("shopUser");
        resp.getWriter().println("退出登录");
    }

}
