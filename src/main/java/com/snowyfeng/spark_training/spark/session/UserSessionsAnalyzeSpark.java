package com.snowyfeng.spark_training.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.snowyfeng.spark_training.conf.ConfigurationManager;
import com.snowyfeng.spark_training.constants.Constants;
import com.snowyfeng.spark_training.dao.SessionAggrStatDao;
import com.snowyfeng.spark_training.dao.TaskDao;
import com.snowyfeng.spark_training.domains.SessionAggrStat;
import com.snowyfeng.spark_training.factory.DAOFactory;
import com.snowyfeng.spark_training.domains.Task;
import com.snowyfeng.spark_training.test.MockData;
import com.snowyfeng.spark_training.utils.DateUtils;
import com.snowyfeng.spark_training.utils.ParamUtils;
import com.snowyfeng.spark_training.utils.StringUtils;
import com.snowyfeng.spark_training.utils.ValidUtils;
import org.apache.spark.Accumulator;
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

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionRDD = filterSessionByParams(sqlContext, sessionid2AggrInfoRDD, taskParams, sessionAggrStatAccumulator);

        JavaPairRDD<String, Row> sessionId2DetailRdd = getSession2DetailRDD(filteredSessionRDD, session2ActionRDD);

        randomExtractSession(sc, task.getTaskId(),
                filteredSessionRDD, sessionId2DetailRdd);

        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());
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
                long stepLength = 0;
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
                    stepLength++;
                }

                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                String value = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + StringUtils.trimComma(searchKeywords.toString()) + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + StringUtils.trimComma(clickCategoryIds.toString()) + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength;
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

    private static JavaPairRDD<String, String> filterSessionByParams(SQLContext sqlContext, JavaPairRDD<String,
            String> sessionid2AggrInfoRDD, final JSONObject taskParams, final Accumulator<String> sessionAggrStatAccumulator) {
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

                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_STEP_LENGTH));
                calculateStepLength(stepLength);
                calculateVisitLength(visitLength);


                return true;
            }

            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }

            }

            private void calculateStepLength(long stepLength) {
                if (stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength > 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });
        return filteredSessionInfoRDD;
    }

    private static JavaPairRDD<String, Row> getSession2DetailRDD(JavaPairRDD<String, String> filteredSessionRDD, JavaPairRDD<String, Row> session2ActionRDD) {

        return null;
    }

    private static void calculateAndPersistAggrStat(String value, long taskId) {
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visitLength_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visitLength_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visitLength_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visitLength_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visitLength_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visitLength_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visitLength_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visitLength_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visitLength_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long stepLength_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long stepLength_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long stepLength_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long stepLength_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long stepLength_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long stepLength_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        double visitLength_1s_3s_ratio = (double) visitLength_1s_3s / (double) session_count;
        double visitLength_4s_6s_ratio = (double) visitLength_4s_6s / (double) session_count;
        double visitLength_7s_9s_ratio = (double) visitLength_7s_9s / (double) session_count;
        double visitLength_10s_30s_ratio = (double) visitLength_10s_30s / (double) session_count;
        double visitLength_30s_60_ratio = (double) visitLength_30s_60s / (double) session_count;
        double visitLength_1m_3m_ratio = (double) visitLength_1m_3m / (double) session_count;
        double visitLength_3m_10m_ratio = (double) visitLength_3m_10m / (double) session_count;
        double visitLength_10m_30m_ratio = (double) visitLength_10m_30m / (double) session_count;
        double visitLength_30m_ratio = (double) visitLength_30m / (double) session_count;

        double stepLength_1_3_ratio = (double) stepLength_1_3 / (double) session_count;
        double stepLength_4_6_ratio = (double) stepLength_4_6 / (double) session_count;
        double stepLength_7_9_ratio = (double) stepLength_7_9 / (double) session_count;
        double stepLength_10_30_ratio = (double) stepLength_10_30 / (double) session_count;
        double stepLength_30_60_ratio = (double) stepLength_30_60 / (double) session_count;
        double stepLength_60_ratio = (double) stepLength_60 / (double) session_count;

        SessionAggrStatDao sessionAggrStatDao = DAOFactory.getSessionAggrStatDao();

        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setStep_length_1_3_ratio(stepLength_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(stepLength_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(stepLength_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(stepLength_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(stepLength_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(stepLength_60_ratio);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visitLength_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visitLength_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visitLength_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visitLength_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visitLength_30s_60_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visitLength_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visitLength_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visitLength_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visitLength_30m_ratio);

        sessionAggrStatDao.insert(sessionAggrStat);

    }

    private static void randomExtractSession(JavaSparkContext sc, long taskId, JavaPairRDD<String, String> filteredSessionRDD, JavaPairRDD<String, Row> sessionId2DetailRdd) {


    }


}
