package com.nexr.dip.tool;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.nexr.dip.Context;
import com.nexr.dip.server.DipContext;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * WFGenenerator generates the workflow definition per each topic.
 */
public class WFGenerator extends AbstractModule{

    public static final String AVRO_TEMPLATE = "camus/camus-avro.properties";
    public static final String TEXT_TEMPLATE = "camus/camus-text.properties";
    public static final String TOPIC_VAR = "${topic}";
    public static final String SCHEMA_REGISTRY_VAR = "${schema-registry}";
    public static final String TASK_COUNT_VAR = "${task-count}";
    public static final String KAFKA_PULL_SIZE_VAR = "${pull-size}";
    public static final String KAFKA_BROKER = "${broker}";
    public static final String ETL_COUNT_DIR = "${count-dir}";
    public static final String ETL_DESTINATION_DIR = "${destination-dir}";
    public static final String EXECUTION_TIMEZONE = "${timezone}";
    public static final String LOGDIR = "${logdir}";
    public static final String DIP_NAMENODE = "${dipNameNode}";
    public static final String DIP_JOBTRACKER = "${dipJobTracker}";
    public static final String DIP_HIVESERVER = "${dipHiveServer}";
    public static final String DIP_USER_NAME = "${userName}";

    @Inject
    private Context context;

    private ExecuteType excuteType;

    private String schemaRegistryUrl;
    private String defaultTaskCount;
    private String kafkaBroker;
    private String timezone;
    private String logDir;
    private String nameNode;
    private String jobTracker;
    private String hiveServer;
    private String etlResultDir;
    private String etlDestinationDir;
    private String kafkaPullSize;
    private String userName;

    private String avroSrcInfos;
    private String textSrcInfos;

    private List<String> avroCamusProperties;
    private List<String> textCamusProperties;
    private List<String> camusLog4jStr;
    private List<String> wfJobProperties;
    private List<String> wfWorkflow;

    public WFGenerator(ExecuteType executeType) throws IOException {
        this.excuteType = executeType;

        avroSrcInfos = context.getConfig(DipContext.AVRO_TOPICS);
        textSrcInfos = context.getConfig(DipContext.TEXT_TOPICS);
        schemaRegistryUrl = context.getConfig("dip.schemaregistry.url");
        defaultTaskCount = context.getConfig("dip.load.task.count");
        kafkaBroker = context.getConfig(DipContext.DIP_KAFKA_BROKER);
        timezone = context.getConfig("dip.load.execution.timezone");
        logDir = context.getConfig("dip.load.logdir");
        nameNode = context.getConfig(DipContext.DIP_NAMENODE);
        jobTracker = context.getConfig(DipContext.DIP_JOBTRACKER);
        hiveServer = context.getConfig(DipContext.DIP_HIVESERVER);
        userName = context.getConfig(DipContext.DIP_USER_NAME);

        etlResultDir = context.getConfig(DipContext.DIP_ETL_COUNT_DIR);
        etlDestinationDir = context.getConfig(DipContext.DIP_ETL_DESTINATION_PATH);
        kafkaPullSize = context.getConfig(DipContext.DIP_KAFKA_PULL_SIZE);

        avroCamusProperties = loadTemplate(AVRO_TEMPLATE);
        textCamusProperties = loadTemplate(TEXT_TEMPLATE);
        camusLog4jStr = loadTemplate("camus/camus-log4j.xml");
        wfJobProperties = loadTemplate("wf/job.properties");
        wfWorkflow = loadTemplate("wf/workflow.xml");

    }

    public static void main(String... args) {

        try {
            ExecuteType type = ExecuteType.Workflow;
            if (args.length > 0) {
                type = ExecuteType.valueOf(args[0]);
            }
            Injector injector = Guice.createInjector(ImmutableList.of(new WFGenerator(type)));
            WFGenerator generator = injector.getInstance(WFGenerator.class);

            generator.generate(generator.getAvroSrcInfos(), generator.getTextSrcInfos());
        } catch (IOException e) {
            System.out.println("Fail to load Template : " + e.getMessage());
            System.exit(1);
        }
    }

    public String getAvroSrcInfos() {
        return avroSrcInfos;
    }

    public String getTextSrcInfos() {
        return textSrcInfos;
    }

    public static List<String> loadTemplate(String src) throws IOException {
        List<String> srcStrs = IOUtils.readLines(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(src));
        return srcStrs;
    }

    public void generate(String avroSrcInfo, String textSrcInfo) throws IOException {
        String[] avros = avroSrcInfo.split(",");
        String[] texts = textSrcInfo.split(",");

        String rootDirName = "apps";
        if (excuteType == ExecuteType.ToolRunner) {
            rootDirName = rootDirName + "-toolrunner";
        }

        File rootDir = new File(rootDirName);
        if (!rootDir.isDirectory()) {
            rootDir.mkdir();
        }
        generateExcutor(rootDir, avros, avroCamusProperties);
        generateExcutor(rootDir, texts, textCamusProperties);

        System.out.println("Create Dir : " + rootDir.getAbsolutePath());
    }

    private void generateExcutor(File parent, String[] srcInfos, List<String> camusProperties) throws IOException {
        for (String srcInfoName : srcInfos) {
            String srcInfo = srcInfoName.trim();
            if (excuteType == ExecuteType.Workflow) {
                generateWorkflowExecutor(srcInfo, parent, camusProperties);
            } else if (excuteType == ExecuteType.ToolRunner) {
                generateToolRunnerExecutor(srcInfo, parent, camusProperties);
            } else {
                System.out.println("No defined excute type, Use Workflow Excutor");
                generateWorkflowExecutor(srcInfo, parent, camusProperties);
            }
        }
    }

    private void generateWorkflowExecutor(String srcInfo, File parent, List<String> camusProperteis) throws IOException {
        File srcInfoDir = new File(parent, srcInfo);
        if (!srcInfoDir.isDirectory()) {
            srcInfoDir.mkdir();
        }
        File libDir = new File(srcInfoDir, "lib");
        if (!libDir.isDirectory()) {
            libDir.mkdir();
        }

        FileWriter propertyWriter = new FileWriter(new File(libDir, "camus.properties"));
        FileWriter log4jWriter = new FileWriter(new File(libDir, "log4j.xml"));
        write(propertyWriter, camusProperteis, srcInfo);
        write(log4jWriter, camusLog4jStr, srcInfo);
        propertyWriter.close();
        log4jWriter.close();

        FileWriter jobPropertyWriter = new FileWriter(new File(srcInfoDir, "job.properties"));
        FileWriter workflowWriter = new FileWriter(new File(srcInfoDir, "workflow.xml"));
        write(jobPropertyWriter, wfJobProperties, srcInfo);
        write(workflowWriter, wfWorkflow, srcInfo);

        jobPropertyWriter.close();
        workflowWriter.close();
    }

    private void generateToolRunnerExecutor(String srcInfo, File parent, List<String> camusProperties) throws IOException {
        File dir = new File(parent, srcInfo);
        if (!dir.isDirectory()) {
            dir.mkdir();
        }
        FileWriter propertyWriter = new FileWriter(new File(dir, "camus.properties"));
        FileWriter log4jWriter = new FileWriter(new File(dir, "log4j.xml"));

        write(propertyWriter, camusProperties, srcInfo);
        write(log4jWriter, camusLog4jStr, srcInfo);

        propertyWriter.close();
        log4jWriter.close();

    }

    private void write(Writer writer, List<String> templates, String srcInfo) throws IOException {
        for (String line : templates) {

            if (line.contains(TOPIC_VAR)) {
                line = line.replace(TOPIC_VAR, srcInfo);
            }
            if (line.contains(SCHEMA_REGISTRY_VAR)) {
                line = line.replace(SCHEMA_REGISTRY_VAR, schemaRegistryUrl);
            }
            if (line.contains(TASK_COUNT_VAR)) {
                String taskCount = context.getConfig("dip.load.task." + srcInfo + ".count");
                if (taskCount != null) {
                    line = line.replace(TASK_COUNT_VAR, taskCount);
                } else {
                    line = line.replace(TASK_COUNT_VAR, defaultTaskCount);
                }
            }
            if (line.contains(KAFKA_BROKER)) {
                line = line.replace(KAFKA_BROKER, kafkaBroker);
            }
            if (line.contains(EXECUTION_TIMEZONE)) {
                line = line.replace(EXECUTION_TIMEZONE, timezone);
            }
            if (line.contains(LOGDIR)) {
                line = line.replace(LOGDIR, logDir);
            }
            if (line.contains(DIP_NAMENODE)) {
                line = line.replace(DIP_NAMENODE, nameNode);
            }
            if (line.contains(DIP_JOBTRACKER)) {
                line = line.replace(DIP_JOBTRACKER, jobTracker);
            }
            if (line.contains(DIP_HIVESERVER)) {
                line = line.replace(DIP_HIVESERVER, hiveServer);
            }
            if (line.contains(DIP_USER_NAME)) {
                line = line.replace(DIP_USER_NAME, userName);
            }
            if (line.contains(ETL_COUNT_DIR)) {
                line = line.replace(ETL_COUNT_DIR, etlResultDir);
            }
            if (line.contains(ETL_DESTINATION_DIR)) {
                line = line.replace(ETL_DESTINATION_DIR, etlDestinationDir );
            }
            if (line.contains(KAFKA_PULL_SIZE_VAR)) {
                String topicKafkaPullSize = context.getConfig("dip.kafka.max.pull.size." + srcInfo);
                if (topicKafkaPullSize != null) {
                    line = line.replace(KAFKA_PULL_SIZE_VAR, topicKafkaPullSize);
                } else {
                    line = line.replace(KAFKA_PULL_SIZE_VAR, kafkaPullSize);
                }
            }

            writer.write(line + "\n");

        }
    }

    @Override
    protected void configure() {
        bind(Context.class).to(DipContext.class).in(Singleton.class);
    }

    public static enum ExecuteType {
        ToolRunner,
        Workflow;
    }

}
