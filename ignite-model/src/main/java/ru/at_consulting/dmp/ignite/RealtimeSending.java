package ru.at_consulting.dmp.ignite;


import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * Target DataMart entity representation
 * Created by OSkoblya on 12.08.2016.
 */
public class RealtimeSending {


    public static class Key {

        //Номер телефона абонента
        @QuerySqlField(name = "MSISDN")
        @AffinityKeyMapped
        private int msisdn;
        //Номер компании
        @QuerySqlField(name = "CAMPAIGN_ID")
        private int campaignId;
        //Идентификатор для определения типа СМС, что будет отправлена абоненту
        @QuerySqlField(name = "SMS_TYPE")
        private String smsType;
        //Дата и время начала события(звоно,смс, интернет)
        @QuerySqlField(name = "STARTTIME")
        private Timestamp startTime;

        public Key(int msisdn, int campaignId, String smsType, Timestamp startTime) {
            this.msisdn = msisdn;
            this.campaignId = campaignId;
            this.smsType = smsType;
            this.startTime = startTime;
        }

        public int getMsisdn() {
            return msisdn;
        }

        public void setMsisdn(int msisdn) {
            this.msisdn = msisdn;
        }

        public int getCampaignId() {
            return campaignId;
        }

        public void setCampaignId(int campaignId) {
            this.campaignId = campaignId;
        }

        public String getSmsType() {
            return smsType;
        }

        public void setSmsType(String smsType) {
            this.smsType = smsType;
        }

        public Timestamp getStartTime() {
            return startTime;
        }

        public void setStartTime(Timestamp startTime) {
            this.startTime = startTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key that = (Key) o;
            return campaignId == that.campaignId &&
                    Objects.equals(msisdn, that.msisdn) &&
                    Objects.equals(smsType, that.smsType) &&
                    Objects.equals(startTime, that.startTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(msisdn, campaignId, smsType, startTime);
        }
    }
    //Уникальный идентификатор абонента
    @QuerySqlField(name = "SUBS_KEY")
    private long subsKey;
    //Дата отправки СМС
    @QuerySqlField(name = "SEND_DT")
    private Timestamp sendDt;
    //Дата доставки СМС
    @QuerySqlField(name = "DELIVERY_DT")
    private Timestamp deliveryDt;
    //Дата начала отправки сообщения для мониторинга ошибок связанных с отправкой смс
    @QuerySqlField(name = "START_SEND_DT")
    private Timestamp startSendDt;
    //Идентификатор статуса (1 - Создано, 2 - Отправлено, 3 - Доставлено, 4 - Не доставлено, 5 - Неизвестный статус, 6 - Просрочено, 7 - Отклонено)
    @QuerySqlField(name = "STATUS_ID")
    private int statusId;
    //Идентификатор сообщения /Код ошибки
    @QuerySqlField(name = "RESULT_ID")
    private long resultId;
    //Текст ошибки
    @QuerySqlField(name = "ERROR_TEXT")
    private String errorText;
    //Допустимое время отставания в секундах между началом события и отправкой СМС
    @QuerySqlField(name = "MAX_TIME_OFFSET")
    private long maxTimeOffset;


    public RealtimeSending(Timestamp sendDt, Timestamp deliveryDt, Timestamp startSendDt, long subsKey,int statusId, long resultId, String errorText, long maxTimeOffset) {
        this.sendDt = sendDt;
        this.subsKey = subsKey;
        this.deliveryDt = deliveryDt;
        this.startSendDt = startSendDt;
        this.statusId = statusId;
        this.resultId = resultId;
        this.errorText = errorText;
        this.maxTimeOffset = maxTimeOffset;
    }

    public Timestamp getSendDt() {
        return sendDt;
    }

    public void setSendDt(Timestamp sendDt) {
        this.sendDt = sendDt;
    }

    public Timestamp getDeliveryDt() {
        return deliveryDt;
    }

    public void setDeliveryDt(Timestamp deliveryDt) {
        this.deliveryDt = deliveryDt;
    }

    public Timestamp getStartSendDt() {
        return startSendDt;
    }

    public void setStartSendDt(Timestamp startSendDt) {
        this.startSendDt = startSendDt;
    }

    public int getStatusId() {
        return statusId;
    }

    public void setStatusId(int statusId) {
        this.statusId = statusId;
    }

    public long getResultId() {
        return resultId;
    }

    public void setResultId(long resultId) {
        this.resultId = resultId;
    }

    public String getErrorText() {
        return errorText;
    }

    public void setErrorText(String errorText) {
        this.errorText = errorText;
    }

    public long getMaxTimeOffset() {
        return maxTimeOffset;
    }

    public void setMaxTimeOffset(long maxTimeOffset) {
        this.maxTimeOffset = maxTimeOffset;
    }

    public long getSubsKey() {
        return subsKey;
    }

    public void setSubsKey(long subsKey) {
        this.subsKey = subsKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RealtimeSending that = (RealtimeSending) o;
        return subsKey == that.subsKey &&
                statusId == that.statusId &&
                resultId == that.resultId &&
                maxTimeOffset == that.maxTimeOffset &&
                Objects.equals(sendDt, that.sendDt) &&
                Objects.equals(deliveryDt, that.deliveryDt) &&
                Objects.equals(startSendDt, that.startSendDt) &&
                Objects.equals(errorText, that.errorText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subsKey, sendDt, deliveryDt, startSendDt, statusId, resultId, errorText, maxTimeOffset);
    }

    @Override
    public String toString() {
        return "RealtimeSending{" +
                "subsKey=" + subsKey +
                ", sendDt=" + sendDt +
                ", deliveryDt=" + deliveryDt +
                ", startSendDt=" + startSendDt +
                ", statusId=" + statusId +
                ", resultId=" + resultId +
                ", errorText='" + errorText + '\'' +
                ", maxTimeOffset=" + maxTimeOffset +
                '}';
    }
}