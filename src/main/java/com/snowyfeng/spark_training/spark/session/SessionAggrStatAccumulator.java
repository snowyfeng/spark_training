package com.snowyfeng.spark_training.spark.session;

import com.snowyfeng.spark_training.constants.Constants;
import com.snowyfeng.spark_training.utils.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * Created by xuxuefeng on 2016-12-1.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    private static final long serialVersionUID = 933940776987631000L;

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String zero(String v) {
        return Constants.SESSION_COUNT + "=0" +
                Constants.TIME_PERIOD_1s_3s + "=0|" +
                Constants.TIME_PERIOD_4s_6s + "=0|" +
                Constants.TIME_PERIOD_7s_9s + "=0|" +
                Constants.TIME_PERIOD_10s_30s + "=0|" +
                Constants.TIME_PERIOD_30s_60s + "=0|" +
                Constants.TIME_PERIOD_1m_3m + "=0|" +
                Constants.TIME_PERIOD_3m_10m + "=0|" +
                Constants.TIME_PERIOD_10m_30m + "=0|" +
                Constants.TIME_PERIOD_30m + "=0";
    }

    private String add(String v1, String v2) {
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (StringUtils.isNotEmpty(oldValue)) {
            int newValue = Integer.valueOf(oldValue) + 1;
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }
}
