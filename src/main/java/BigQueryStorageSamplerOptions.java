import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Optional;

public class BigQueryStorageSamplerOptions {

  public static class ChannelPoolOptions {
    public static final String MIN_RPCS_PER_CHANNEL_OPT = "channel_pool_options.min_rpcs_per_channel";
    public static final String MAX_RPCS_PER_CHANNEL_OPT = "channel_pool_options.max_rpcs_per_channel";
    public static final String MIN_CHANNEL_COUNT_OPT = "channel_pool_options.min_channel_count";
    public static final String MAX_CHANNEL_COUNT_OPT = "channel_pool_options.max_channel_count";
    public static final String INITIAL_CHANNEL_COUNT_OPT = "channel_pool_options.initial_channel_count";

    private final Optional<Integer> minRpcsPerChannel;
    private final Optional<Integer> maxRpcsPerChannel;
    private final Optional<Integer> minChannelCount;
    private final Optional<Integer> maxChannelCount;
    private final Optional<Integer> initialChannelCount;

    public ChannelPoolOptions(CommandLine commandLine) {
      this.minRpcsPerChannel = getOptionValue(commandLine, MIN_RPCS_PER_CHANNEL_OPT);
      this.maxRpcsPerChannel = getOptionValue(commandLine, MAX_RPCS_PER_CHANNEL_OPT);
      this.minChannelCount = getOptionValue(commandLine, MIN_CHANNEL_COUNT_OPT);
      this.maxChannelCount = getOptionValue(commandLine, MAX_CHANNEL_COUNT_OPT);
      this.initialChannelCount = getOptionValue(commandLine, INITIAL_CHANNEL_COUNT_OPT);
    }

    private Optional<Integer> getOptionValue(CommandLine commandLine, String optionName) {
      return commandLine.hasOption(optionName)
          ? Optional.of(Integer.parseInt(commandLine.getOptionValue(optionName)))
          : Optional.empty();
    }

    public Optional<Integer> getMinRpcsPerChannel() {
      return this.minRpcsPerChannel;
    }

    public Optional<Integer> getMaxRpcsPerChannel() {
      return this.maxRpcsPerChannel;
    }

    public Optional<Integer> getMinChannelCount() {
      return this.minChannelCount;
    }

    public Optional<Integer> getMaxChannelCount() {
      return this.maxChannelCount;
    }

    public Optional<Integer> getInitialChannelCount() {
      return this.initialChannelCount;
    }
  }

  public static final String PARENT_OPT = "parent";
  public static final String TABLE_OPT = "table";
  public static final String FORMAT_OPT = "format";
  public static final String MAX_STREAMS_OPT = "max_streams";
  public static final String ENDPOINT_OPT = "endpoint";
  public static final String CHANNELS_PER_CPU_OPT = "channels_per_cpu";
  public static final String EXECUTOR_THREAD_COUNT_OPT = "executor_thread_count";
  public static final String FLOW_CONTROL_WINDOW_SIZE_OPT = "flow_control_window_size";

  private String parent;
  private String table;
  private String format;
  private Integer maxStreams;
  private Optional<String> endpoint;
  private Optional<Float> channelsPerCpu;
  private ChannelPoolOptions channelPoolOptions;
  private Optional<Integer> executorThreadCount;
  private Optional<Integer> flowControlWindowSize;

  public BigQueryStorageSamplerOptions(String[] args) {
    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine commandLine = parser.parse(getParseOptions(), args);
      initializeFromCommandLine(commandLine);
    } catch (ParseException parseException) {
      throw new RuntimeException(parseException);
    }
  }

  private Options getParseOptions() {
    Options parseOptions = new Options();
    parseOptions.addOption(
        Option.builder("p")
            .longOpt(PARENT_OPT)
            .desc("The ID of the parent project for the read session")
            .required()
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder("t")
            .longOpt(TABLE_OPT)
            .desc("The fully-qualified ID of the table to read from")
            .required()
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder("f")
            .longOpt(FORMAT_OPT)
            .desc("The format of the data (Avro or Arrow)")
            .required()
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder("s")
            .longOpt(MAX_STREAMS_OPT)
            .desc("The maximum number of streams to request during session creation")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(ENDPOINT_OPT)
            .desc("The read API endpoint for the operation")
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(CHANNELS_PER_CPU_OPT)
            .desc("The number of channels to configure per CPU in GAPIC")
            .hasArg()
            .type(Float.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(ChannelPoolOptions.MIN_RPCS_PER_CHANNEL_OPT)
            .desc("The minimum number of RPCs per gRPC sub-channel")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(ChannelPoolOptions.MAX_RPCS_PER_CHANNEL_OPT)
            .desc("The maximum number of RPCs per gRPC sub-channel")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(ChannelPoolOptions.MIN_CHANNEL_COUNT_OPT)
            .desc("The minimum number of gRPC sub-channels")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(ChannelPoolOptions.MAX_CHANNEL_COUNT_OPT)
            .desc("The maximum number of gRPC sub-channels")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(ChannelPoolOptions.INITIAL_CHANNEL_COUNT_OPT)
            .desc("The initial number of gRPC sub-channels")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(EXECUTOR_THREAD_COUNT_OPT)
            .desc("The (fixed) number of threads to create in the executor pool")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt(FLOW_CONTROL_WINDOW_SIZE_OPT)
            .desc("The size of the initial gRPC flow control window")
            .hasArg()
            .type(Integer.class)
            .build());
    return parseOptions;
  }

  private void initializeFromCommandLine(CommandLine commandLine) {
    this.parent = commandLine.getOptionValue(PARENT_OPT);
    this.table = commandLine.getOptionValue(TABLE_OPT);
    this.format = commandLine.getOptionValue(FORMAT_OPT);
    this.maxStreams = Integer.parseInt(commandLine.getOptionValue(MAX_STREAMS_OPT, "1"));
    this.endpoint = Optional.ofNullable(commandLine.getOptionValue(ENDPOINT_OPT));
    this.channelsPerCpu =
        commandLine.hasOption(CHANNELS_PER_CPU_OPT)
            ? Optional.of(Float.parseFloat(commandLine.getOptionValue(CHANNELS_PER_CPU_OPT)))
            : Optional.empty();
    this.channelPoolOptions = new ChannelPoolOptions(commandLine);
    this.executorThreadCount =
        commandLine.hasOption(EXECUTOR_THREAD_COUNT_OPT)
            ? Optional.of(Integer.parseInt(commandLine.getOptionValue(EXECUTOR_THREAD_COUNT_OPT)))
            : Optional.empty();
    this.flowControlWindowSize =
        commandLine.hasOption(FLOW_CONTROL_WINDOW_SIZE_OPT)
            ? Optional.of(Integer.parseInt(commandLine.getOptionValue(FLOW_CONTROL_WINDOW_SIZE_OPT)))
            : Optional.empty();
  }

  public String getParent() {
    return this.parent;
  }

  public String getTable() {
    return this.table;
  }

  public String getFormat() {
    return this.format;
  }

  public Integer getMaxStreams() {
    return this.maxStreams;
  }

  public Optional<String> getEndpoint() {
    return this.endpoint;
  }

  public Optional<Float> getChannelsPerCpu() {
    return this.channelsPerCpu;
  }

  public ChannelPoolOptions getChannelPoolOptions() {
    return this.channelPoolOptions;
  }

  public Optional<Integer> getExecutorThreadCount() {
    return this.executorThreadCount;
  }

  public Optional<Integer> getFlowControlWindowSize() {
    return this.flowControlWindowSize;
  }
}
