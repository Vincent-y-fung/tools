package org.example;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class JsonComparator {
    // 配置常量
    private static final long MEMORY_THRESHOLD = Runtime.getRuntime().maxMemory() / 4; // 可用内存阈值
    private static final int BATCH_SIZE = 1000;    // 差异批处理大小
    private static final ObjectMapper OBJECT_MAPPER = createConfiguredMapper();
    private static final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );

    // 新增：列表排序策略配置
    private static final boolean ENABLE_LIST_SORTING = true; // 是否启用列表排序
    private static final String ID_FIELD = "id"; // 默认ID字段名

    // 新增：内存监控器
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    // 差异处理模式
    public enum CompareMode {
        TREE,       // 树模式（适合中小JSON）
        STREAMING,  // 流模式（适合大JSON）
        HYBRID      // 混合模式（自适应）
    }

    public enum DifferenceType {
        VALUE_CHANGED,    // 值修改
        NODE_ADDED,       // 新增节点
        NODE_REMOVED,     // 删除节点
        TYPE_MISMATCH     // 类型不匹配
    }

    public static class Difference {
        private final String path;          // JSON路径（如: "user.address[0].street"）
        private final DifferenceType type;  // 差异类型
        private final JsonNode oldValue;    // 旧值（可能为null）
        private final JsonNode newValue;    // 新值（可能为null）

        public Difference(String path, DifferenceType type, JsonNode oldValue, JsonNode newValue) {
            this.path = path;
            this.type = type;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        // Getters
        public String getPath() { return path; }
        public DifferenceType getType() { return type; }
        public JsonNode getOldValue() { return oldValue; }
        public JsonNode getNewValue() { return newValue; }

        @Override
        public String toString() {
            return "Difference{" +
                    "path='" + path + '\'' +
                    ", type=" + type +
                    ", oldValue=" + oldValue +
                    ", newValue=" + newValue +
                    '}';
        }
    }

    private static ObjectMapper createConfiguredMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        return mapper;
    }

    /**
     * 对比两个JSON字符串（支持自适应深度和内存管理）
     * @param oldJson 旧JSON字符串
     * @param newJson 新JSON字符串
     * @param mode 对比模式
     * @return 差异列表
     */
    public static List<Difference> compare(String oldJson, String newJson, CompareMode mode) throws JsonProcessingException {
        switch (mode) {
            case TREE:
                return compareTreeMode(oldJson, newJson);
            case STREAMING:
                return compareLargeJson(oldJson, newJson, Integer.MAX_VALUE);
            case HYBRID:
            default:
                return compareHybridMode(oldJson, newJson);
        }
    }

    /**
     * 树模式对比（完整加载到内存）
     */
    private static List<Difference> compareTreeMode(String oldJson, String newJson) throws JsonProcessingException {
        JsonNode oldNode = OBJECT_MAPPER.readTree(oldJson);
        JsonNode newNode = OBJECT_MAPPER.readTree(newJson);

        // 估算JSON深度
        int estimatedDepth = estimateJsonDepth(oldNode, newNode);

        // 根据估算深度选择策略
        if (estimatedDepth > 50) {
            System.out.println("警告: 检测到深度较大的JSON结构(" + estimatedDepth + "层)，建议使用STREAMING模式");
        }

        return compareWithAdaptiveDepth(oldNode, newNode, "", 0);
    }

    /**
     * 混合模式对比（根据内存使用情况自适应）
     */
    private static List<Difference> compareHybridMode(String oldJson, String newJson) {
        List<Difference> differences = new ArrayList<>();

        // 首先尝试树模式
        try {
            return compareTreeMode(oldJson, newJson);
        } catch (OutOfMemoryError | JsonProcessingException e) {
            System.err.println("树模式对比失败，切换到流模式: " + e.getMessage());
            // 清理内存
            System.gc();
            // 使用流模式重试
            return compareLargeJson(oldJson, newJson, Integer.MAX_VALUE);
        }
    }

    /**
     * 估算JSON最大深度
     */
    private static int estimateJsonDepth(JsonNode... nodes) {
        int maxDepth = 0;
        for (JsonNode node : nodes) {
            maxDepth = Math.max(maxDepth, calculateNodeDepth(node));
        }
        return maxDepth;
    }

    /**
     * 递归计算节点深度
     */
    private static int calculateNodeDepth(JsonNode node) {
        if (node == null || node.isValueNode()) {
            return 0;
        }

        int maxChildDepth = 0;
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                JsonNode child = fields.next().getValue();
                maxChildDepth = Math.max(maxChildDepth, calculateNodeDepth(child));
            }
        } else if (node.isArray()) {
            for (JsonNode element : node) {
                maxChildDepth = Math.max(maxChildDepth, calculateNodeDepth(element));
            }
        }

        return maxChildDepth + 1;
    }

    /**
     * 处理大型JSON文件的对比
     */
    private static List<Difference> compareLargeJson(String oldJson, String newJson, int depthLimit) {
        List<Difference> differences = new ArrayList<>();
        try (JsonParser oldParser = OBJECT_MAPPER.getFactory().createParser(oldJson);
             JsonParser newParser = OBJECT_MAPPER.getFactory().createParser(newJson)) {

            // 同步解析两个JSON流
            compareJsonStreams(oldParser, newParser, depthLimit, differences::add);

        } catch (IOException e) {
            throw new RuntimeException("JSON解析错误: " + e.getMessage(), e);
        }
        return differences;
    }

    /**
     * 同步解析两个JSON流并对比
     */
    private static void compareJsonStreams(JsonParser oldParser, JsonParser newParser,
                                           int depthLimit, Consumer<Difference> diffConsumer) throws IOException {
        // 记录当前路径和深度
        Deque<String> pathStack = new ArrayDeque<>();
        int currentDepth = 0;

        // 初始化解析器
        JsonToken oldToken = oldParser.nextToken();
        JsonToken newToken = newParser.nextToken();

        while (oldToken != null || newToken != null) {
            // 处理深度限制
            if (currentDepth > depthLimit) {
                if (oldToken != null) oldToken = skipChildren(oldParser, oldToken);
                if (newToken != null) newToken = skipChildren(newParser, newToken);
                continue;
            }

            // 处理路径和类型差异
            if (oldToken == null) {
                diffConsumer.accept(createAddedNodeDifference(pathStack, newParser, newToken));
                newToken = newParser.nextToken();
                continue;
            }

            if (newToken == null) {
                diffConsumer.accept(createRemovedNodeDifference(pathStack, oldParser, oldToken));
                oldToken = oldParser.nextToken();
                continue;
            }

            // 处理令牌类型差异
            if (oldToken != newToken) {
                diffConsumer.accept(new Difference(
                        buildPath(pathStack),
                        DifferenceType.TYPE_MISMATCH,
                        readValueAsTree(oldParser, oldToken),
                        readValueAsTree(newParser, newToken)
                ));

                // 同步解析器位置
                oldToken = oldParser.nextToken();
                newToken = newParser.nextToken();
                continue;
            }

            // 根据令牌类型处理
            switch (oldToken) {
                case START_OBJECT:
                    currentDepth++;
                    pathStack.push("{}");
                    oldToken = oldParser.nextToken();
                    newToken = newParser.nextToken();
                    break;

                case END_OBJECT:
                    currentDepth--;
                    pathStack.pop();
                    oldToken = oldParser.nextToken();
                    newToken = newParser.nextToken();
                    break;

                case START_ARRAY:
                    currentDepth++;
                    pathStack.push("[]");
                    oldToken = oldParser.nextToken();
                    newToken = newParser.nextToken();
                    break;

                case END_ARRAY:
                    currentDepth--;
                    pathStack.pop();
                    oldToken = oldParser.nextToken();
                    newToken = newParser.nextToken();
                    break;

                case FIELD_NAME:
                    String fieldName = oldParser.getCurrentName();
                    pathStack.push(fieldName);
                    oldToken = oldParser.nextToken();
                    newToken = newParser.nextToken();
                    break;

                default: // 值节点
                    // 对比值
                    JsonNode oldValue = readValueAsTree(oldParser, oldToken);
                    JsonNode newValue = readValueAsTree(newParser, newToken);

                    if (!oldValue.equals(newValue)) {
                        diffConsumer.accept(new Difference(
                                buildPath(pathStack),
                                DifferenceType.VALUE_CHANGED,
                                oldValue,
                                newValue
                        ));
                    }

                    // 从路径栈中弹出字段名
                    if (!pathStack.isEmpty() && !"[]".equals(pathStack.peek()) && !"{}".equals(pathStack.peek())) {
                        pathStack.pop();
                    }

                    oldToken = oldParser.nextToken();
                    newToken = newParser.nextToken();
            }
        }
    }

    /**
     * 创建新增节点的差异对象
     */
    private static Difference createAddedNodeDifference(Deque<String> pathStack, JsonParser parser, JsonToken token) throws IOException {
        String path = buildPath(pathStack);
        JsonNode value = readValueAsTree(parser, token);
        return new Difference(path, DifferenceType.NODE_ADDED, null, value);
    }

    /**
     * 创建删除节点的差异对象
     */
    private static Difference createRemovedNodeDifference(Deque<String> pathStack, JsonParser parser, JsonToken token) throws IOException {
        String path = buildPath(pathStack);
        JsonNode value = readValueAsTree(parser, token);
        return new Difference(path, DifferenceType.NODE_REMOVED, value, null);
    }

    /**
     * 从解析器读取当前值作为JsonNode
     */
    private static JsonNode readValueAsTree(JsonParser parser, JsonToken token) throws IOException {
        if (token == null) return null;

        // 保存当前位置
        JsonLocation currentLocation = parser.getCurrentLocation();

        // 解析值
        JsonNode node = OBJECT_MAPPER.readTree(parser);

        // 如果解析器提前结束，重置到当前位置
        if (parser.getCurrentLocation().equals(currentLocation)) {
            parser.nextToken();
        }

        return node;
    }

    /**
     * 跳过当前节点的所有子节点
     */
    private static JsonToken skipChildren(JsonParser parser, JsonToken token) throws IOException {
        if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
            return parser.skipChildren().nextToken();
        }
        return parser.nextToken();
    }

    /**
     * 构建当前路径字符串
     */
    private static String buildPath(Deque<String> pathStack) {
        if (pathStack.isEmpty()) return "";

        List<String> pathElements = new ArrayList<>(pathStack);
        StringBuilder pathBuilder = new StringBuilder();

        for (int i = 0; i < pathElements.size(); i++) {
            String element = pathElements.get(i);

            if ("[]".equals(element)) {
                // 数组标记，跳过
                continue;
            }

            if ("{}".equals(element)) {
                // 对象标记，跳过
                continue;
            }

            if (i > 0 && pathElements.get(i-1).equals("[]")) {
                // 数组元素
                pathBuilder.append("[").append(element).append("]");
            } else {
                // 对象字段
                if (pathBuilder.length() > 0) {
                    pathBuilder.append(".");
                }
                pathBuilder.append(element);
            }
        }

        return pathBuilder.toString();
    }

    /**
     * 自适应深度限制的对比方法
     */
    private static List<Difference> compareWithAdaptiveDepth(JsonNode oldNode, JsonNode newNode,
                                                             String currentPath, int currentDepth) {
        // 监控内存使用情况
        if (currentDepth % 10 == 0 && isMemoryThresholdExceeded()) {
            System.out.println("内存使用接近阈值，当前深度: " + currentDepth);
            // 当内存不足时，提前返回差异
            List<Difference> differences = new ArrayList<>();
            if (!oldNode.equals(newNode)) {
                differences.add(new Difference(currentPath, DifferenceType.VALUE_CHANGED, oldNode, newNode));
            }
            return differences;
        }

        // 基础类型直接对比
        if (oldNode.isValueNode() && newNode.isValueNode()) {
            List<Difference> differences = new ArrayList<>();
            if (!oldNode.equals(newNode)) {
                differences.add(new Difference(currentPath, DifferenceType.VALUE_CHANGED, oldNode, newNode));
            }
            return differences;
        }

        // 类型不匹配直接记录
        if (oldNode.getNodeType() != newNode.getNodeType()) {
            return Collections.singletonList(
                    new Difference(currentPath, DifferenceType.TYPE_MISMATCH, oldNode, newNode)
            );
        }

        // 对象节点处理
        if (oldNode.isObject() && newNode.isObject()) {
            return handleObjectNodesAdaptive((ObjectNode) oldNode, (ObjectNode) newNode, currentPath, currentDepth);
        }

        // 数组节点处理
        if (oldNode.isArray() && newNode.isArray()) {
            return handleArrayNodesAdaptive((ArrayNode) oldNode, (ArrayNode) newNode, currentPath, currentDepth);
        }

        return Collections.emptyList();
    }

    /**
     * 处理对象节点
     */
    private static List<Difference> handleObjectNodesAdaptive(ObjectNode oldObj, ObjectNode newObj,
                                                              String path, int currentDepth) {
        List<Difference> differences = new ArrayList<>();

        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(keysToSet(oldObj));
        allKeys.addAll(keysToSet(newObj));

        for (String key : allKeys) {
            String newPath = path.isEmpty() ? key : path + "." + key;
            JsonNode oldChild = oldObj.get(key);
            JsonNode newChild = newObj.get(key);

            if (oldChild == null) {
                differences.add(new Difference(newPath, DifferenceType.NODE_ADDED, null, newChild));
            } else if (newChild == null) {
                differences.add(new Difference(newPath, DifferenceType.NODE_REMOVED, oldChild, null));
            } else {
                differences.addAll(compareWithAdaptiveDepth(oldChild, newChild, newPath, currentDepth + 1));
            }
        }

        return differences;
    }

    /**
     * 处理数组节点（支持排序）
     */
    private static List<Difference> handleArrayNodesAdaptive(ArrayNode oldArr, ArrayNode newArr,
                                                             String path, int currentDepth) {
        List<Difference> differences = new ArrayList<>();

        // 如果启用排序且数组元素是对象类型，尝试排序
        if (ENABLE_LIST_SORTING && oldArr.size() > 0 && newArr.size() > 0 &&
                oldArr.get(0).isObject() && newArr.get(0).isObject()) {

            // 克隆数组避免修改原始数据
            ArrayNode sortedOldArr = cloneAndSortArray(oldArr);
            ArrayNode sortedNewArr = cloneAndSortArray(newArr);

            int maxLength = Math.max(sortedOldArr.size(), sortedNewArr.size());
            for (int i = 0; i < maxLength; i++) {
                String indexPath = path + "[" + i + "]";
                JsonNode oldElement = i < sortedOldArr.size() ? sortedOldArr.get(i) : null;
                JsonNode newElement = i < sortedNewArr.size() ? sortedNewArr.get(i) : null;

                if (oldElement == null) {
                    differences.add(new Difference(indexPath, DifferenceType.NODE_ADDED, null, newElement));
                } else if (newElement == null) {
                    differences.add(new Difference(indexPath, DifferenceType.NODE_REMOVED, oldElement, null));
                } else {
                    differences.addAll(compareWithAdaptiveDepth(oldElement, newElement, indexPath, currentDepth + 1));
                }
            }
        } else {
            // 普通数组对比（按顺序）
            int maxLength = Math.max(oldArr.size(), newArr.size());
            for (int i = 0; i < maxLength; i++) {
                String indexPath = path + "[" + i + "]";
                JsonNode oldElement = i < oldArr.size() ? oldArr.get(i) : null;
                JsonNode newElement = i < newArr.size() ? newArr.get(i) : null;

                if (oldElement == null) {
                    differences.add(new Difference(indexPath, DifferenceType.NODE_ADDED, null, newElement));
                } else if (newElement == null) {
                    differences.add(new Difference(indexPath, DifferenceType.NODE_REMOVED, oldElement, null));
                } else {
                    differences.addAll(compareWithAdaptiveDepth(oldElement, newElement, indexPath, currentDepth + 1));
                }
            }
        }

        return differences;
    }

    /**
     * 克隆并排序数组
     */
    private static ArrayNode cloneAndSortArray(ArrayNode original) {
        // 创建数组副本
        ArrayNode cloned = OBJECT_MAPPER.createArrayNode();

        // 复制元素
        for (JsonNode element : original) {
            cloned.add(element.deepCopy());
        }

        // 排序数组
        try {
            // 尝试基于ID字段排序
            sortArrayById(cloned);
        } catch (Exception e) {
            // 如果ID排序失败，尝试基于内容排序
            try {
                sortArrayByContent(cloned);
            } catch (Exception ex) {
                // 如果内容排序也失败，使用原始顺序
                System.err.println("警告: 数组排序失败，使用原始顺序: " + ex.getMessage());
            }
        }

        return cloned;
    }

    /**
     * 基于ID字段排序数组
     */
    private static void sortArrayById(ArrayNode array) {
        List<JsonNode> list = new ArrayList<>();
        for (JsonNode element : array) {
            list.add(element);
        }

        list.sort((o1, o2) -> {
            if (o1.has(ID_FIELD) && o2.has(ID_FIELD)) {
                JsonNode id1 = o1.get(ID_FIELD);
                JsonNode id2 = o2.get(ID_FIELD);

                if (id1.isTextual() && id2.isTextual()) {
                    return id1.asText().compareTo(id2.asText());
                } else if (id1.isNumber() && id2.isNumber()) {
                    return Double.compare(id1.asDouble(), id2.asDouble());
                }
            }
            return 0;
        });

        // 清空原数组并添加排序后的元素
        array.removeAll();
        for (JsonNode element : list) {
            array.add(element);
        }
    }

    /**
     * 基于内容排序数组
     */
    private static void sortArrayByContent(ArrayNode array) {
        List<JsonNode> list = new ArrayList<>();
        for (JsonNode element : array) {
            list.add(element);
        }

        list.sort((o1, o2) -> {
            try {
                // 将JSON节点转换为规范化字符串进行比较
                String str1 = OBJECT_MAPPER.writeValueAsString(o1);
                String str2 = OBJECT_MAPPER.writeValueAsString(o2);
                return str1.compareTo(str2);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("JSON序列化失败: " + e.getMessage(), e);
            }
        });

        // 清空原数组并添加排序后的元素
        array.removeAll();
        for (JsonNode element : list) {
            array.add(element);
        }
    }

    /**
     * 检查是否超过内存阈值
     */
    private static boolean isMemoryThresholdExceeded() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        long usedMemory = heapUsage.getUsed();
        return usedMemory > MEMORY_THRESHOLD;
    }

    /**
     * 将ObjectNode的键转换为Set
     */
    private static Set<String> keysToSet(ObjectNode node) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.fieldNames(), Spliterator.ORDERED), false)
                .collect(Collectors.toSet());    }

    // 示例用法
    public static void main(String[] args) throws JsonProcessingException {
        String oldJson = """
        {
            "user": {
                "name": "Alice",
                "age": 30,
                "hobbies": ["reading", "cycling"],
                "friends": [
                    {"id": 2, "name": "Bob"},
                    {"id": 1, "name": "Charlie"}
                ]
            }
        }""";

        String newJson = """
        {
            "user": {
                "name": "Alice",
                "age": 31,
                "hobbies": ["reading", "swimming"],
                "friends": [
                    {"id": 1, "name": "Charlie"},
                    {"id": 2, "name": "Bob"},
                    {"id": 3, "name": "David"}
                ]
            }
        }""";

        List<Difference> diffs = JsonComparator.compare(oldJson, newJson, CompareMode.HYBRID);
        diffs.forEach(System.out::println);
    }
}