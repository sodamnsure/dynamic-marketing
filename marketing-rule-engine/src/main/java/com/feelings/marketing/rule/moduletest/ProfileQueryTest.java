package com.feelings.marketing.rule.moduletest;

import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.service.UserProfileQueryServiceHbaseImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/7 4:33 下午
 * @desc: 画像条件查询服务模块测试类
 */
public class ProfileQueryTest {
    @Test
    public void testQueryProfile() throws IOException {
        // 构造参数
        HashMap<String, String> userProfileParams = new HashMap<>();
        userProfileParams.put("k12", "v117");
        userProfileParams.put("k22", "v978");

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserProfileParams(userProfileParams);

        // 构造查询服务
        UserProfileQueryServiceHbaseImpl impl = new UserProfileQueryServiceHbaseImpl();
        boolean flag = impl.judgeProfileCondition("000645", ruleParam);
        System.out.println(flag);
    }
}
