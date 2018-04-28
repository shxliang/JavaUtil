package dao.impl;

import dao.BaseMySQLDao;

/**
 * Created by Macan on 2018/2/24.
 * mysql 查询实现
 */
public class JudgeDao extends BaseMySQLDao {

    /**
     * 插入案件判决书到数据库中
     * @param caseID 案件ID
     * @param judgement 审判文书
     * @return 受影响的条数
     */
    public int addJudgement(String caseID, String judgement) {
        String sql = "insert into caseInfo(caseID, judgeText) value(?, ?)";
        return executeUpdate(sql, caseID, judgement);
    }

    public int addJudgement(String sql) {
        return executeUpdate(sql);
    }

}
