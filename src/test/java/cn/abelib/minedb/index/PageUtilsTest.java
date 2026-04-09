package cn.abelib.minedb.index;

import cn.abelib.minedb.utils.KeyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class PageUtilsTest {
    private List<KeyValue> keyValues;

    @Before
    public void init() {
        keyValues = new ArrayList<>();
        keyValues.add(new KeyValue("a", "1"));
        keyValues.add(new KeyValue("c", "3"));
        keyValues.add(new KeyValue("e", "5"));
        keyValues.add(new KeyValue("g", "7"));
    }

    @Test
    public void midIndexOddTest() {
        List<KeyValue> oddList = new ArrayList<>(keyValues);
        oddList.add(new KeyValue("i", "9")); // 5 elements

        // mid index for 5 elements should be 2
        Assert.assertEquals(2, PageUtils.midIndex(createMockNode(oddList)));
    }

    @Test
    public void midIndexEvenTest() {
        // mid index for 4 elements should be 2
        Assert.assertEquals(2, PageUtils.midIndex(createMockNode(keyValues)));
    }

    @Test
    public void midIndexEmptyTest() {
        List<KeyValue> emptyList = new ArrayList<>();
        Assert.assertEquals(0, PageUtils.midIndex(createMockNode(emptyList)));
    }

    @Test
    public void midIndexSingleTest() {
        List<KeyValue> singleList = new ArrayList<>();
        singleList.add(new KeyValue("a", "1"));
        Assert.assertEquals(0, PageUtils.midIndex(createMockNode(singleList)));
    }

    @Test
    public void binarySearchForIndexFirstTest() {
        // "a" is equal to first element, algorithm returns position after it (for insertion)
        KeyValue entry = new KeyValue("a", "");
        int index = PageUtils.binarySearchForIndex(entry, keyValues);
        Assert.assertEquals(1, index);
    }

    @Test
    public void binarySearchForIndexLastTest() {
        KeyValue entry = new KeyValue("h", "");
        int index = PageUtils.binarySearchForIndex(entry, keyValues);
        Assert.assertEquals(4, index);
    }

    @Test
    public void binarySearchForIndexMiddleTest() {
        KeyValue entry = new KeyValue("d", "");
        int index = PageUtils.binarySearchForIndex(entry, keyValues);
        Assert.assertEquals(2, index);
    }

    @Test
    public void binarySearchForIndexExactMatchTest() {
        // "c" exists at index 1, algorithm returns position after it
        KeyValue entry = new KeyValue("c", "");
        int index = PageUtils.binarySearchForIndex(entry, keyValues);
        Assert.assertEquals(2, index);
    }

    @Test
    public void binarySearchForIndexBeforeFirstTest() {
        // Key before the first element
        KeyValue entry = new KeyValue("0", "");
        int index = PageUtils.binarySearchForIndex(entry, keyValues);
        Assert.assertEquals(0, index);
    }

    @Test
    public void binarySearchFoundTest() {
        KeyValue entry = new KeyValue("e", "5");
        int index = PageUtils.binarySearch(entry, keyValues);
        Assert.assertEquals(2, index);
    }

    @Test
    public void binarySearchNotFoundTest() {
        KeyValue entry = new KeyValue("b", "");
        int index = PageUtils.binarySearch(entry, keyValues);
        Assert.assertEquals(-1, index);
    }

    @Test
    public void calculateRecordSizeTest() {
        KeyValue kv = new KeyValue("test", "value");
        int size = PageUtils.calculateRecordSize(kv);

        // key=4, value=5
        // header(16) + key(4) + valueSize(4) + value(5) = 29
        Assert.assertEquals(29, size);
    }

    @Test
    public void calculateRecordSizeEmptyValueTest() {
        KeyValue kv = new KeyValue("test", "");
        int size = PageUtils.calculateRecordSize(kv);

        // key=4, value=0
        // header(16) + key(4) + valueSize(4) + value(0) = 24
        Assert.assertEquals(24, size);
    }

    @Test
    public void calculateUsedSpaceTest() throws Exception {
        TreeNode node = new TreeNode(new Configuration(), true, true, 0);
        node.getKeyValues().add(new KeyValue("a", "1"));
        node.getKeyValues().add(new KeyValue("b", "2"));

        int usedSpace = PageUtils.calculateUsedSpace(node);
        Assert.assertTrue(usedSpace > 0);
    }

    @Test
    public void getAvailableDataSpaceTest() {
        Configuration conf = new Configuration();
        int available = PageUtils.getAvailableDataSpace(conf);

        // pageSize(16KB) - headerSize(128) = 16256
        Assert.assertEquals(16 * 1024 - 128, available);
    }

    @Test
    public void nodeTypeSpecialTest() throws Exception {
        TreeNode node = new TreeNode(new Configuration(), true, true, 0);
        Assert.assertEquals(PageUtils.NodeType.SPECIAL_NODE, PageUtils.nodeType(node));
    }

    @Test
    public void nodeTypeRootTest() throws Exception {
        TreeNode node = new TreeNode(new Configuration(), false, true, 0);
        Assert.assertEquals(PageUtils.NodeType.ROOT_NODE, PageUtils.nodeType(node));
    }

    @Test
    public void nodeTypeLeafTest() throws Exception {
        TreeNode node = new TreeNode(new Configuration(), true, false, 0);
        Assert.assertEquals(PageUtils.NodeType.LEAF_NODE, PageUtils.nodeType(node));
    }

    @Test
    public void nodeTypeIndexTest() throws Exception {
        TreeNode node = new TreeNode(new Configuration(), false, false, 0);
        Assert.assertEquals(PageUtils.NodeType.INDEX_NODE, PageUtils.nodeType(node));
    }

    private TreeNode createMockNode(List<KeyValue> kvs) {
        TreeNode node = new TreeNode();
        node.setKeys(kvs);
        node.setConfiguration(new Configuration());
        return node;
    }
}
