package ideal.sylph.runner.flink.udf.aggfunctions;


import ideal.sylph.runner.flink.udf.UdfAnnotation;
import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Accumulator for latest.
 */

/**
 * 最后所在的h3区域内停留时长和状态
 * 返回h3_no，最后状态，停留时长，是否接过单，车辆电量
 */
@UdfAnnotation(name = "driver_last_status")
public class DriverLastStatus extends AggregateFunction<Object[], DriverLastStatus.LastDriverStatusData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverLastStatus.class);

    public class LastDriverStatusData {
        public Long h3No = 0l;
        public Integer status = 0;
        // 区域第一次记录时间戳
        public Timestamp firstTs;
        // 区域最后一次记录时间戳
        public Timestamp lastTs;
        public Integer haveAcceptOrder = 0;
        public Double power = 0.0;
    }


    @Override
    public LastDriverStatusData createAccumulator() {
        return new LastDriverStatusData();
    }

    @Override
    public Object[] getValue(LastDriverStatusData data) {
        Object[] objects = new Object[5];
        objects[0] = data.h3No;
        objects[1] = data.status;
        objects[2] = data.lastTs.getTime() - data.firstTs.getTime();
        objects[3] = data.haveAcceptOrder;
        objects[4] = data.power;
//        objects[0] = String.valueOf(data.h3No);
//        objects[1] = String.valueOf(data.status);
//        objects[2] = String.valueOf(data.lastTs.getTime() - data.firstTs.getTime());
//        objects[3] = String.valueOf(data.haveAcceptOrder);
//        objects[4] = String.valueOf(data.power);
        return objects;
    }

    // input: h3_no, status, timestamp, power
    public void accumulate(LastDriverStatusData data, Object... input) {
        Long h3No = (Long) input[0];
        Integer status = (Integer) input[1];
        Timestamp ts = (Timestamp) input[2];
        Double power = (Double) input[3];
        if (ts == null) {
            LOGGER.warn("异常数据,h3No:{},status:{},ts:null,power:{}", h3No, status, power);
            return;
        }

        // 如果区域变更了，则更新首次时间
        if (h3No != null && !h3No.equals(data.h3No)) {
            data.h3No = h3No;
            data.firstTs = ts;
            data.haveAcceptOrder = 0;
        }

        // init value
        if (data.firstTs == null) {
            data.firstTs = ts;
            data.haveAcceptOrder = 0;
        }

        data.status = status;
        data.lastTs = ts;
        data.power = power;

        // 如果当前状态为4（服务中），则修改是否接过单haveAcceptOrder为1
        if (data.haveAcceptOrder == 0 && (status != null && status == 4)) {
            data.haveAcceptOrder = 1;
        }
    }
}