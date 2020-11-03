package cn.abelib.minedb.index;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * @Author: abel.huang
 * @Date: 2020-10-25 01:13
 */
public class BalanceTree {
    /**
     * 根节点
     */
    private MetaNode metaNode;
    private TreeNode root;
    /**
     * 配置文件
     */
    private Configuration configuration;

    public BalanceTree(int degree) {
        this.configuration = new Configuration(degree);
    }

    public BalanceTree(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.init();
    }

    public void insert(String key, String value) throws Exception {
        if (Objects.isNull(metaNode)) {
            throw new IllegalArgumentException("Invalid meta data");
        } else if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException("Invalid blank key");
        }

    }

    /**
     * 初始化
     */
    private void init() throws IOException {
        if (Objects.isNull(this.metaNode)) {
            this.metaNode = new MetaNode(configuration);
        }
        if (Objects.isNull(this.root)) {
            this.root = new TreeNode(configuration, true, true, 0L);
        }
    }

    public MetaNode getMetaNode() {
        return metaNode;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
