package edu.berkeley.cs186.database.memory;

/**
 * Buffer frame.
 * Buffer Frame（缓冲帧） 是内存中用于缓存磁盘页面的基本单位。
 * 它是磁盘页面在内存中的"容器"，包含了页面数据以及管理这些数据所需的元信息。
 *
 */
abstract class BufferFrame {
    Object tag = null;
    private int pinCount = 0;

    /**
     * Pin buffer frame; cannot be evicted while pinned. A "hit" happens when the
     * buffer frame gets pinned.
     * 机制：防止正在使用的页面被淘汰
     */
    void pin() {
        ++pinCount;
    }

    /**
     * Unpin buffer frame.
     */
    void unpin() {
        if (!isPinned()) {
            throw new IllegalStateException("cannot unpin unpinned frame");
        }
        --pinCount;
    }

    /**
     * @return whether this frame is pinned
     */
    boolean isPinned() {
        return pinCount > 0;
    }

    /**
     * @return whether this frame is valid
     */
    abstract boolean isValid();

    /**
     * @return page number of this frame
     * 建立内存Frame与磁盘页面的映射关系
     */
    abstract long getPageNum();

    /**
     * Flushes this buffer frame to disk, but does not unload it.
     */
    abstract void flush();

    /**
     * Read from the buffer frame.
     * @param position position in buffer frame to start reading
     * @param num number of bytes to read
     * @param buf output buffer
     */
    abstract void readBytes(short position, short num, byte[] buf);

    /**
     * Write to the buffer frame, and mark frame as dirtied.
     * @param position position in buffer frame to start writing
     * @param num number of bytes to write
     * @param buf input buffer
     */
    abstract void writeBytes(short position, short num, byte[] buf);

    /**
     * Requests a valid Frame object for the page (if invalid, a new Frame object is returned).
     * Frame is pinned on return.
     */
    abstract BufferFrame requestValidFrame();

    /**
     * @return amount of space available to user of the frame
     */
    short getEffectivePageSize() {
        return BufferManager.EFFECTIVE_PAGE_SIZE;
    }

    /**
     * @param pageLSN new pageLSN of the page loaded in this frame
     */
    abstract void setPageLSN(long pageLSN);

    /**
     * @return pageLSN of the page loaded in this frame
     */
    abstract long getPageLSN();
}
