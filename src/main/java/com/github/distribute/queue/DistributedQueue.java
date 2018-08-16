package com.github.distribute.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式队列，同步队列的实现
 * 
 * @author linbingwen
 *
 * @param <T>
 */
public class DistributedQueue<T> {
	private static Logger logger = LoggerFactory.getLogger(DistributedQueue.class);

	protected final ZooKeeper zooKeeper;// ���ڲ���zookeeper��Ⱥ
	protected final String root;// ����ڵ�
	private int queueSize;
	private String startPath = "/queue/start";

	protected static final String Node_NAME = "n_";// ˳��ڵ�����

	public DistributedQueue(ZooKeeper zooKeeper, String root, int queueSize) {
		this.zooKeeper = zooKeeper;
		this.root = root;
		this.queueSize = queueSize;
		init();
	}

	/**
	 * ��ʼ����Ŀ¼
	 */
	private void init() {
		try {
			Stat stat = zooKeeper.exists(root, false);// �ж�һ�¸�Ŀ¼�Ƿ����
			if (stat == null) {
				zooKeeper.create(root, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			zooKeeper.delete(startPath, -1); // ɾ�������ı�־
		} catch (Exception e) {
			logger.error("create rootPath error", e);
		}
	}

	/**
	 * ��ȡ���еĴ�С
	 * 
	 * @return
	 * @throws Exception
	 */
	public int size() throws Exception {
		return zooKeeper.getChildren(root, false).size();
	}

	/**
	 * �ж϶����Ƿ�Ϊ��
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean isEmpty() throws Exception {
		return zooKeeper.getChildren(root, false).size() == 0;
	}

	/**
	 * bytes תobject
	 * 
	 * @param bytes
	 * @return
	 */
	private Object ByteToObject(byte[] bytes) {
		Object obj = null;
		try {
			// bytearray to object
			ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
			ObjectInputStream oi = new ObjectInputStream(bi);

			obj = oi.readObject();
			bi.close();
			oi.close();
		} catch (Exception e) {
			logger.error("translation" + e.getMessage());
			e.printStackTrace();
		}
		return obj;
	}

	/**
	 * Object תbyte
	 * 
	 * @param obj
	 * @return
	 */
	private byte[] ObjectToByte(java.lang.Object obj) {
		byte[] bytes = null;
		try {
			// object to bytearray
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bo);
			oo.writeObject(obj);

			bytes = bo.toByteArray();

			bo.close();
			oo.close();
		} catch (Exception e) {
			logger.error("translation" + e.getMessage());
			e.printStackTrace();
		}
		return bytes;
	}

	/**
	 * ������ṩ���,������Ļ�������ȴ�ֱ��start��־λ���
	 * 
	 * @param element
	 * @return
	 * @throws Exception
	 */
	public boolean offer(T element) throws Exception {
		// ������ݽڵ������·��
		String nodeFullPath = root.concat("/").concat(Node_NAME);
		try {
			if (queueSize > size()) {
				// �����־õĽڵ㣬д�����
				zooKeeper.create(nodeFullPath, ObjectToByte(element), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				// ���ж�һ�¶����Ƿ���
				if (queueSize > size()) {
					zooKeeper.delete(startPath, -1); // ȷ��������
				} else {
					zooKeeper.create(startPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} else {
				// ����������ı��
				if (zooKeeper.exists(startPath, false) != null) {
					zooKeeper.create(startPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}

				final CountDownLatch latch = new CountDownLatch(1);
				final Watcher previousListener = new Watcher() {
					public void process(WatchedEvent event) {
						if (event.getType() == EventType.NodeDeleted) {
							latch.countDown();
						}
					}
				};

				// ���ڵ㲻���ڻ�����쳣
				zooKeeper.exists(startPath, previousListener);
				latch.await();
				offer(element);

			}
		} catch (ZkNoNodeException e) {
			logger.error("", e);
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}
		return true;
	}

	/**
	 * �Ӷ���ȡ���,����start��־λʱ����ʼȡ��ݣ�ȫ��ȡ����ݺ��ɾ��start��־
	 * 
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public T poll() throws Exception {

		try {
			// ���л�û��
			if (zooKeeper.exists(startPath, false) == null) {
				final CountDownLatch latch = new CountDownLatch(1);
				final Watcher previousListener = new Watcher() {
					public void process(WatchedEvent event) {
						if (event.getType() == EventType.NodeCreated) {
							latch.countDown();
						}
					}
				};

				// ���ڵ㲻���ڻ�����쳣
				zooKeeper.exists(startPath, previousListener);

				// ���ڵ㲻���ڻ�����쳣
				latch.await();
			}

			List<String> list = zooKeeper.getChildren(root, false);
			if (list.size() == 0) {
				return null;
			}
			// �����а�����С�����˳������
			Collections.sort(list, new Comparator<String>() {
				public int compare(String lhs, String rhs) {
					return getNodeNumber(lhs, Node_NAME).compareTo(getNodeNumber(rhs, Node_NAME));
				}
			});

			/**
			 * �������е�Ԫ����ѭ����Ȼ�󹹽������·������ͨ�����·��ȥ��ȡ���
			 */
			for (String nodeName : list) {
				String nodeFullPath = root.concat("/").concat(nodeName);
				try {
					T node = (T) ByteToObject(zooKeeper.getData(nodeFullPath, false, null));
					zooKeeper.delete(nodeFullPath, -1);
					return node;
				} catch (ZkNoNodeException e) {
					logger.error("", e);
				}
			}
			return null;
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}

	}

	/**
	 * ��ȡ�ڵ�����ֵķ���
	 * 
	 * @param str
	 * @param nodeName
	 * @return
	 */
	private String getNodeNumber(String str, String nodeName) {
		int index = str.lastIndexOf(nodeName);
		if (index >= 0) {
			index += Node_NAME.length();
			return index <= str.length() ? str.substring(index) : "";
		}
		return str;

	}

}
