package com.snowyfeng.spark_training.spark;

import com.alibaba.fastjson.JSONObject;
import com.snowyfeng.spark_training.conf.ConfigurationManager;
import com.snowyfeng.spark_training.constants.Constants;
import com.snowyfeng.spark_training.dao.TaskDao;
import com.snowyfeng.spark_training.dao.impl.DAOFactory;
import com.snowyfeng.spark_training.domains.Task;
import com.snowyfeng.spark_training.test.MockData;
import com.snowyfeng.spark_training.utils.DateUtils;
import com.snowyfeng.spark_training.utils.ParamUtils;
import com.snowyfeng.spark_training.utils.StringUtils;
import com.snowyfeng.spark_training.utils.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class UserSessionsAnalyzeSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc);

        final long taskId = ParamUtils.getTaskIdFromArgs(args, null);

        TaskDao taskDao = DAOFactory.getTaskDao();
        Task task = taskDao.findById(taskId);
        MockData.mock(sc, sqlContext);

        JSONObject taskParams = JSONObject.parseObject(task.getTaskParam());

        JavaPairRDD<String, Row> session2ActionRDD = getActionRDDByDateRange(sqlContext, taskParams);

        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext, session2ActionRDD);

        JavaPairRDD<String, String> filteredSessionRDD = filterSessionByParams(sqlContext, sessionid2AggrInfoRDD, taskParams);


        sc.stop();
    }


    private static SQLContext getSQLContext(JavaSparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    private static JavaPairRDD<String, Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParams) {
        String startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_AGE);
        String endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE);
        String sql = "select *from user_visit_action where date>=" + startDate
                + " and date<= " + endDate;
        JavaRDD<Row> actionRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

            private static final long serialVersionUID = 6877931168276651871L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
        return session2ActionRDD;
    }

    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaPairRDD<String, Row> session2ActionRDD) {

        JavaPairRDD<String, String> partInfoRDD = session2ActionRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            private static final long serialVersionUID = -3577846111095835782L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;
                Iterator<Row> rows = tuple._2.iterator();
                StringBuffer searchKeywords = new StringBuffer("");
                StringBuffer clickCategoryIds = new StringBuffer("");
                Date startTime = null;
                Date endTime = null;
                String userId = null;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    Date action_time = DateUtils.parseTime(row.getString(4));
                    String searchKeyword = row.getString(5);
                    String clickCategoryId = row.getString(6);
                    if (StringUtils.isEmpty(userId)) {
                        userId = row.getString(1);
                    }
                    if (null == startTime) {
                        startTime = action_time;
                    }
                    if (null == endTime) {
                        endTime = action_time;
                    }
                    if (action_time.before(startTime)) {
                        startTime = action_time;
                    }
                    if (action_time.after(endTime)) {
                        endTime = action_time;
                    }
                    if (StringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywords.toString().contains(searchKeyword)) {
                            searchKeywords.append("," + searchKeyword);
                        }
                    }

                    if (StringUtils.isNotEmpty(clickCategoryId)) {
                        if (!clickCategoryIds.toString().contains(clickCategoryId)) {
                            clickCategoryIds.append("," + clickCategoryId);
                        }
                    }
                }

                String value = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + StringUtils.trimComma(searchKeywords.toString()) + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + StringUtils.trimComma(clickCategoryIds.toString());
                return new Tuple2<String, String>(userId, value);
            }
        });

        String sql = "select *from user_info";
        JavaPairRDD<String, Row> userInfoRDD = sqlContext.sql(sql).javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 6556560572380213819L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(0), row);
            }
        });

        JavaPairRDD<String, String> session2AllInfo = partInfoRDD.join(userInfoRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, String>() {
            private static final long serialVersionUID = -7003290815556814830L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                String partInfo = tuple._2._1;
                Row row = tuple._2._2;
                String sessionId = StringUtils.getFieldFromConcatString(partInfo, "\\|", Constants.FIELD_SESSION_ID);
                int age = row.getInt(3);
                String professional = row.getString(4);
                String city = row.getString(5);
                String sex = row.getString(6);
                String value = partInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;
                return new Tuple2<String, String>(sessionId, value);
            }
        });
        return session2AllInfo;
    }

    private static JavaPairRDD<String, String> filterSessionByParams(SQLContext sqlContext, JavaPairRDD<String, String> sessionid2AggrInfoRDD, JSONObject taskParams) {
        String startAge = ParamUtils.getParam(taskParams, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParams, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParams, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParams, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParams, Constants.PARAM_SEX);
        String searchKeywords = ParamUtils.getParam(taskParams, Constants.PARAM_KEYWORDS);
        String clickCategoryIds = ParamUtils.getParam(taskParams, Constants.PARAM_CATEGORY_IDS);
        String _parameters = (startAge == null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : null)
                + (endAge == null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : null)
                + (professionals == null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : null)
                + (cities == null ? Constants.PARAM_CITIES + "=" + cities + "|" : null)
                + (sex == null ? Constants.PARAM_SEX + "=" + sex + "|" : null)
                + (searchKeywords == null ? Constants.PARAM_CATEGORY_IDS + "=" + searchKeywords + "|" : null)
                + (clickCategoryIds == null ? Constants.PARAM_CATEGORY_IDS + "=" + clickCategoryIds + "|" : null);

        if (_parameters.endsWith("\\|")) {
            _parameters = _parameters.substring(0, _parameters.length() - 1);
        }
        final String parameters = _parameters;

        JavaPairRDD<String, String> filteredSessionInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String info = tuple._2;
                //valid age
                if (!ValidUtils.between(info, Constants.FIELD_AGE, parameters, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }
                //valid PROFESSIONALS
                if (!ValidUtils.in(info, Constants.FIELD_PROFESSIONAL, parameters, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }
                //city
                if (!ValidUtils.in(info, Constants.FIELD_CITY, parameters, Constants.PARAM_CITIES)) {
                    return false;
                }
                //sex
                if (!ValidUtils.equal(info, Constants.FIELD_CITY, parameters, Constants.PARAM_CITIES)) {
                    return false;
                }
                //seachKeyword
                if (!ValidUtils.in(info, Constants.FIELD_SEARCH_KEYWORDS, parameters, Constants.PARAM_KEYWORDS)) {
                    return false;
                }
                //click categoryIds
                if (!ValidUtils.in(info, Constants.FIELD_CLICK_CATEGORY_IDS, parameters, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }
                return true;
            }
        });
        return filteredSessionInfoRDD;
    }

}
