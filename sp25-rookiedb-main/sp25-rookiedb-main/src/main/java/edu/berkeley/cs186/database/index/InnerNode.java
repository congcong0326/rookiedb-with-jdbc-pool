package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A inner node of a B+ tree. Every inner node in a B+ tree of order d stores
 * between d and 2d keys. An inner node with n keys stores n + 1 "pointers" to
 * children nodes (where a pointer is just a page number). Moreover, every
 * inner node is serialized and persisted on a single page; see toBytes and
 * fromBytes for details on how an inner node is serialized. For example, here
 * is an illustration of an order 2 inner node:
 *
 *     +----+----+----+----+
 *     | 10 | 20 | 30 |    |
 *     +----+----+----+----+
 *    /     |    |     \
 *
 *   内部节点存储n个键，n+1个指针，n <= 2d
 *   关注节点如何持久化与序列化
 */
class InnerNode extends BPlusNode {
    // Metadata about the B+ tree that this node belongs to.
    /**
     * - metadata.getOrder() 获取树的阶数d
     * - metadata.getKeySchema() 获取键的数据类型
     * - metadata.getRootPageNum() 获取根节点页号
     */
    private BPlusTreeMetadata metadata;

    // Buffer manager
    /**
     * - 页面的获取/释放（fetchPage/unpin）
     * - 内存与磁盘的数据同步（sync操作）
     * - 新页面的分配（fetchNewPage）
     */
    private BufferManager bufferManager;

    // Lock context of the B+ tree
    /**
     * - 整棵树的锁粒度控制（通过disableChildLocks）
     * - 事务并发访问的隔离性保证
     * - 操作时的锁升级/降级管理
     */
    private LockContext treeContext;

    // The page on which this leaf is serialized.
    /**
     * - 节点所在的物理页号（getPageNum()）
     * - 页面内容的缓冲区（getBuffer()）
     * - 脏页标记和持久化机制（通过sync方法）
     */
    private Page page;

    // The keys and child pointers of this inner node. See the comment above
    // LeafNode.keys and LeafNode.rids in LeafNode.java for a warning on the
    // difference between the keys and children here versus the keys and children
    // stored on disk. `keys` is always stored in ascending order.
    /**
     *         [key1, key2]          <- InnerNode
     *        /     |      \
     *   child0  child1  child2      <- 子节点（可能是 InnerNode 或 LeafNode）
     */
    private List<DataBox> keys;//路由键值列表
    private List<Long> children;//子节点指针列表

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Construct a brand new inner node.
     */
    InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
              List<Long> children, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
             keys, children, treeContext);
    }

    /**
     * Construct an inner node that is persisted to page `page`.
     */
    private InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                      List<DataBox> keys, List<Long> children, LockContext treeContext) {
        try {
            assert (keys.size() <= 2 * metadata.getOrder());
            assert (keys.size() + 1 == children.size());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.children = new ArrayList<>(children);
            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(DataBox key) {
        // TODO(proj2): implement
        // InnerNode 直接返回LeafNode，说明是递归调用，从InnerNode节点开始搜索，找出LeafNode
        // InnerNode 节点的结构为，所以需要找出其child
        // *     +----+----+----+----+
        // *     | 10 | 20 | 30 |    |  keys
        // *     +----+----+----+----+
        // *    /     |    |     \      child
        // todo 好像没考虑节点不存在情况
        // 找到第一个大于等于key的child节点
        if (keys.isEmpty()) {
            return null;
        }
        int childIndex = numLessThanEqual(key, keys);
        // 通过childIndex索引到childPage，然后从磁盘置换出下一个节点
        BPlusNode child = getChild(childIndex);
        // 这里设计得还是挺秒，不需要做类型检查，如果递归到LeafNode，会返回自己
        return child.get(key);
    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf() {
        assert(children.size() > 0);
        // TODO(proj2): implement
        // 获取左子树
        BPlusNode leftChild = getChild(0);
        return leftChild.getLeftmostLeaf();
    }

    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // TODO(proj2): implement
        // 返回的最后一个小于key的坐标，注意该坐标是 Child 的坐标
        int index = numLessThanEqual(key, keys);
        BPlusNode putChild = getChild(index);
        Optional<Pair<DataBox, Long>> splitNode = putChild.put(key, rid);
        // 节点分裂了，返回了新的中间节点，需要给它新建立一个索引指向它
        if (splitNode.isPresent()) {
            DataBox middleNodeKey = splitNode.get().getFirst();
            Long rightNodePageNum = splitNode.get().getSecond();
            //
            int d = metadata.getOrder();
            // 插入的key比index中的key大，返回的右子树的数据都大于key
            // 所以应该将新的rightNodeKey 追加到key的位置后面
            // 注意 index 是Child 的坐标，而不是 key 在 keys 中的坐标
            int middleIndex = numLessThanEqual(middleNodeKey, keys);
            // 插入中间节点
            keys.add(middleIndex, middleNodeKey);
            // 插入下层右子树的页索引
            children.add(middleIndex + 1, rightNodePageNum);
            if (keys.size() <= 2 * d) {
                sync();
                return Optional.empty();
            }
            // 裂开
            else {
                return splitInnerNode(d);
            }
        }
        return Optional.empty();
    }

    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor) {
        // TODO(proj2): implement
        int d = metadata.getOrder();
        while (data.hasNext()) {
            BPlusNode child = getChild(children.size() - 1);
            if (keys.size() < 2 * d) {
                // 尽可能尝试给右子树添加
                Optional<Pair<DataBox, Long>> splitNode  = child.bulkLoad(data, fillFactor);
                // 有上提的节点
                if (splitNode.isPresent()) {
                    DataBox middleNodeData = splitNode.get().getFirst();
                    Long rightNodeIndex = splitNode.get().getSecond();
                    int index = numLessThanEqual(middleNodeData, keys);
                    keys.add(index, middleNodeData);
                    children.add(index + 1, rightNodeIndex);
                    sync();
                }
            }
            // 还有数据未填充，但是此时该节点已经满了
            else {
                return splitInnerNode(d);
            }
        }

        return Optional.empty();
    }

    private Optional<Pair<DataBox, Long>> splitInnerNode(int d) {
        // 节点已经满，需要分裂，找出需要上提的key
        DataBox promoteKey = keys.get(d);

        // 分割键列表（排除提升键）
        List<DataBox> leftKeys = new ArrayList<>(keys.subList(0, d));
        List<DataBox> rightKeys = new ArrayList<>(keys.subList(d + 1, keys.size()));

        // 分割子指针（包含中间键对应的指针）
        List<Long> leftChildren = new ArrayList<>(children.subList(0, d + 1));
        List<Long> rightChildren = new ArrayList<>(children.subList(d + 1, children.size()));
        //创建右节点
        InnerNode newNode = new InnerNode(metadata, bufferManager, rightKeys, rightChildren, treeContext);
        // 更新当前节点
        this.keys = leftKeys;
        this.children = leftChildren;
        sync();

        // 返回分裂信息，让上层处理
        return Optional.of(new Pair<>(promoteKey, newNode.getPage().getPageNum()));
    }

    // See BPlusNode.remove.
    @Override
    public void remove(DataBox key) {
        // TODO(proj2): implement
        // 找到叶子节点，将请求委托给叶子节点执行
        LeafNode leafNode = get(key);
        leafNode.remove(key);
        return;
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    private BPlusNode getChild(int i) {
        long pageNum = children.get(i);
        return BPlusNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

    /**
     * 将脏页刷入磁盘
     * 刷入之前会做脏页检查
     */
    private void sync() {
        page.pin();// 锁定页面防止换出
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();// 将当前节点数据序列化为字节数组
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);// 读取当前页面的原始数据

            // 仅当内存数据与磁盘不一致时写入
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();// 确保无论如何都释放页面锁
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<Long> getChildren() {
        return children;
    }
    /**
     * Returns the largest number d such that the serialization of an InnerNode
     * with 2d keys will fit on a single page.
     * 给定一个页的大小，计算能够容纳多少个条目 n ，将 n / 2 得到 B+ 树的 阶
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 4 + (n * keySize) + ((n + 1) * 8)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - 8 is the number of bytes used to store a child pointer.
        //
        // Solving the following equation
        //
        //   5 + (n * keySize) + ((n + 1) * 8) <= pageSizeInBytes
        //
        // we get
        //
        //   n = (pageSizeInBytes - 13) / (keySize + 8)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + 8);
        return n / 2;
    }

    /**
     * Given a list ys sorted in ascending order, numLessThanEqual(x, ys) returns
     * the number of elements in ys that are less than or equal to x. For
     * example,
     *
     *   numLessThanEqual(0, Arrays.asList(1, 2, 3, 4, 5)) == 0
     *   numLessThanEqual(1, Arrays.asList(1, 2, 3, 4, 5)) == 1
     *   numLessThanEqual(2, Arrays.asList(1, 2, 3, 4, 5)) == 2
     *   numLessThanEqual(3, Arrays.asList(1, 2, 3, 4, 5)) == 3
     *   numLessThanEqual(4, Arrays.asList(1, 2, 3, 4, 5)) == 4
     *   numLessThanEqual(5, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *   numLessThanEqual(6, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *
     * This helper function is useful when we're navigating down a B+ tree and
     * need to decide which child to visit. For example, imagine an index node
     * with the following 4 keys and 5 children pointers:
     *
     *     +---+---+---+---+
     *     | a | b | c | d |
     *     +---+---+---+---+
     *    /    |   |   |    \
     *   0     1   2   3     4
     *
     * If we're searching the tree for value c, then we need to visit child 3.
     * Not coincidentally, there are also 3 values less than or equal to c (i.e.
     * a, b, c).
     * 用来确认子节点的索引
     * 比如在键列表[10,20,30,40]查找key=25时需要返回2，对应child 2
     *
     */
    static <T extends Comparable<T>> int numLessThanEqual(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) <= 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    /**
     * 返回 ys 中小于 x 的第一个元素小婊
     * ys 1 2 3 4 5
     * x 2 return 1
     * 适合找出 B+ 树中的left节点
     * 比如在键列表[10,20,30,40]查找key=20时需要返回1，确认大于等于key的左区间
     * @param x
     * @param ys
     * @return
     * @param <T>
     */
    static <T extends Comparable<T>> int numLessThan(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) < 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(children.get(i)).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(children.get(children.size() - 1)).append(")");
        return sb.toString();
    }

    @Override
    public String toSexp() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(getChild(i).toSexp()).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(getChild(children.size() - 1).toSexp()).append(")");
        return sb.toString();
    }

    /**
     * An inner node on page 0 with a single key k and two children on page 1 and
     * 2 is turned into the following DOT fragment:
     *
     *   node0[label = "<f0>|k|<f1>"];
     *   ... // children
     *   "node0":f0 -> "node1";
     *   "node0":f1 -> "node2";
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("<f%d>", i));
            ss.add(keys.get(i).toString());
        }
        ss.add(String.format("<f%d>", keys.size()));

        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        String node = String.format("  node%d[label = \"%s\"];", pageNum, s);

        List<String> lines = new ArrayList<>();
        lines.add(node);
        for (int i = 0; i < children.size(); ++i) {
            BPlusNode child = getChild(i);
            long childPageNum = child.getPage().getPageNum();
            lines.add(child.toDot());
            lines.add(String.format("  \"node%d\":f%d -> \"node%d\";",
                                    pageNum, i, childPageNum));
        }

        return String.join("\n", lines);
    }

    // Serialization ///////////////////////////////////////////////////////////

    /**
     * java object 到字节数组的映射关系
     * @return
     */
    @Override
    public byte[] toBytes() {
        // When we serialize an inner node, we write:
        //
        //   a. the literal value 0 (1 byte) which indicates that this node is not
        //      a leaf node,
        //   b. the number n (4 bytes) of keys this inner node contains (which is
        //      one fewer than the number of children pointers),
        //   c. the n keys, and
        //   d. the n+1 children pointers.
        //
        // For example, the following bytes:
        //
        //   +----+-------------+----+-------------------------+-------------------------+
        //   | 00 | 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03 | 00 00 00 00 00 00 00 07 |
        //   +----+-------------+----+-------------------------+-------------------------+
        //    \__/ \___________/ \__/ \_________________________________________________/
        //     a         b        c                           d
        //
        // represent an inner node with one key (i.e. 1) and two children pointers
        // (i.e. page 3 and page 7).

        // All sizes are in bytes.
        assert (keys.size() <= 2 * metadata.getOrder());
        assert (keys.size() + 1 == children.size());
        int isLeafSize = 1;
        int numKeysSize = Integer.BYTES;
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int childrenSize = Long.BYTES * children.size();
        int size = isLeafSize + numKeysSize + keysSize + childrenSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (Long child : children) {
            buf.putLong(child);
        }
        return buf.array();
    }

    /**
     * Loads an inner node from page `pageNum`.
     * LeafNode 的 fromBytes 可以参考 InnerNode的实现
     */
    public static InnerNode fromBytes(BPlusTreeMetadata metadata,
                                      BufferManager bufferManager, LockContext treeContext, long pageNum) {
        // 从磁盘加载一页到内存
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buf = page.getBuffer();
        // 判断节点类型是否是inner node
        byte nodeType = buf.get();
        assert(nodeType == (byte) 0);

        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        // 读取int，32 位 4 字节
        int n = buf.getInt();
        // 读取键
        for (int i = 0; i < n; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        // 读取指针
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getLong());
        }
        // 调用构造函数
        return new InnerNode(metadata, bufferManager, page, keys, children, treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InnerNode)) {
            return false;
        }
        InnerNode n = (InnerNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
               keys.equals(n.keys) &&
               children.equals(n.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, children);
    }
}
