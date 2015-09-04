package com.nexr.dip.loader;

import com.nexr.dip.common.Utils;
import org.apache.oozie.client.WorkflowJob;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import java.sql.Timestamp;

@Entity
@NamedQueries({
        @NamedQuery(name = "GET_LOADRESULT", query = "select a.name, a.executionTime, a.statusStr, a.jobId, a.wfStatusStr, " +
                "a.externalId, a.etlExecutionPath, a.countFile, a.count, a.errorCount, a.error, a.resultFiles, a.endTime " +
                "from LoadResult a " +
                "where a.name = :name and a.executionTime = :executionTime" ),
        @NamedQuery(name = "GET_LOADRESULT_BY_TOPIC", query = "select a.name, a.executionTime, a.statusStr, a.jobId, a.wfStatusStr, " +
                "a.externalId, a.etlExecutionPath, a.countFile, a.count, a.errorCount, a.error, a.resultFiles, a.endTime " +
                "from LoadResult a " +
                "where a.name = :name order by a.executionTime desc" ),
        @NamedQuery(name = "GET_LOADRESULT_FROM_TIME", query = "select a.name, a.executionTime, a.statusStr, a.jobId, a.wfStatusStr, " +
                "a.externalId, a.etlExecutionPath, a.countFile, a.count, a.errorCount, a.error, a.resultFiles, a.endTime " +
                "from LoadResult a " +
                "where a.executionTime >= :executionTime order by a.executionTime desc, a.name desc"),
        @NamedQuery(name = "UPDATE_LOADRESULT", query = "update LoadResult a set a.statusStr = :statusStr, a.jobId = :jobId, a.wfStatusStr = :wfStatusStr, " +
                "a.externalId = :externalId, a.etlExecutionPath = :etlExecutionPath, a.countFile = :countFile, a.count = :count, a.errorCount = :errorCount, " +
                "a.error = :error, a.resultFiles = :resultFiles, a.endTime = :endTime where a.name = :name and a.executionTime = :executionTime")
})
@Table(name = "loadresult")
@XmlRootElement
public class LoadResult {
    @Column(name = "name")
    private String name;
    @Column(name = "job_id")
    private String jobId;
    @Column(name = "external_id")
    private String externalId;
    @Column(name = "execution_time")
    private Timestamp executionTime;
    @Column(name = "end_time")
    private Timestamp endTime;
    @Column(name = "wfstatus")
    private String wfStatusStr;
    @Column(name = "error")
    private String error;
    @Column(name = "count_file")
    private String countFile;
    @Column(name = "count", nullable = false)
    private long count;
    @Column(name = "error_count", nullable = false)
    private long errorCount = -1;
    @Column(name = "result_files", length = 6000)
    private String resultFiles;
    @Column(name = "status")
    private String statusStr;
    @Column(name = "etl_execution_path")
    private String etlExecutionPath;

    public LoadResult() {

    }

    public LoadResult(String name, String jobId, Timestamp executionTime) {
        this.name = name;
        this.jobId = jobId;
        this.executionTime = executionTime;
        setStatusStr(STATUS.PREP.toString());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public WorkflowJob.Status getWfStatus() {
        return WorkflowJob.Status.valueOf(wfStatusStr);
    }

    public void setWfStatus(WorkflowJob.Status wfStatus) {
        if (wfStatus != null) {
            this.wfStatusStr = wfStatus.toString();
        }
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Timestamp getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(Timestamp executionTime) {
        this.executionTime = executionTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getCountFile() {
        return countFile;
    }

    public void setCountFile(String countFile) {
        this.countFile = countFile;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public String getResultFiles() {
        return resultFiles;
    }

    public void setResultFiles(String resultFiles) {
        this.resultFiles = resultFiles;
    }

    public int getResultFileCount() {
        return (resultFiles == null || resultFiles.isEmpty()) ? 0 : resultFiles.split(",").length;
    }

    public STATUS getStatus() {
        return STATUS.valueOf(this.statusStr);
    }

    public void setStatus(STATUS status) {
        this.statusStr = status.toString();
    }

    public String getWfStatusStr() {
        return wfStatusStr;
    }

    public void setWfStatusStr(String wfStatusStr) {
        this.wfStatusStr = wfStatusStr;
    }

    public String getStatusStr() {
        return statusStr;
    }

    public void setStatusStr(String statusStr) {
        this.statusStr = statusStr;
    }

    public String getEtlExecutionPath() {
        return etlExecutionPath;
    }

    public void setEtlExecutionPath(String etlExecutionPath) {
        this.etlExecutionPath = etlExecutionPath;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[" + name + "] ");
        builder.append(Utils.getDateString(executionTime.getTime()));
        if(endTime != null) {
            builder.append(", " + Utils.getDateString(endTime.getTime()));
        }
        builder.append(", Status=" + statusStr);
        builder.append(", jobId=" + jobId);
        builder.append(", wfStatus=" + wfStatusStr);
        builder.append(", externalId=" + externalId);
        builder.append(", etlExecutionPath=" + etlExecutionPath);
        builder.append(", countFile=" + countFile);
        builder.append(", count=" + count);
        builder.append(", errorCount=" + errorCount);
        if (error != null) {
            builder.append(",  error=" + error);
        }
        builder.append(", resultFiles=(" + getResultFileCount() + ") " + resultFiles);
        return builder.toString();
    }

    public enum STATUS {
        PREP,
        RUNNING, // not yet finished
        WF_DONE_COUNT_FAILED, // WF finished but counting fail
        RETRY, // Need to retry
        SUCCEEDED, //
        FAILED; //
    }
}
