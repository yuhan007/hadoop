package com.github.distribute.queue;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.distribute.lock.zookeeper.BaseDistributedLock;

/**
 * 
 * @author linbingwen
 *
 * @param <T>
 */
public class DistributedSimpleQueue<T> {

	private static Logger logger = LoggerFactory.getLogger(BaseDistributedLock.class);

	protected final ZkClient zkClient;//���ڲ���zookeeper��Ⱥ
	protected final String root;//����ڵ�

	protected static final String Node_NAME = "n_";//˳��ڵ�����
	


	public DistributedSimpleQueue(ZkClient zkClient, String root) {
		this.zkClient = zkClient;
		this.root = root;
	}
    
	//��ȡ���еĴ�С
	public int size() {
		/**
		 * ͨ���ȡ��ڵ��µ��ӽڵ��б�
		 */
		return zkClient.getChildren(root).size();
	}
	
    //�ж϶����Ƿ�Ϊ��
	public boolean isEmpty() {
		return zkClient.getChildren(root).size() == 0;
	}
	
	/**
	 * ������ṩ���
	 * @param element
	 * @return
	 * @throws Exception
	 */
    public boolean offer(T element) throws Exception{
    	
    	//������ݽڵ������·��
    	String nodeFullPath = root .concat( "/" ).concat( Node_NAME );
        try {
        	//�����־õĽڵ㣬д�����
            zkClient.createPersistentSequential(nodeFullPath , element);
        }catch (ZkNoNodeException e) {
        	zkClient.createPersistent(root);
        	offer(element);
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
        return true;
    }


    //�Ӷ���ȡ���
	@SuppressWarnings("unchecked")
	public T poll() throws Exception {
		
		try {

			List<String> list = zkClient.getChildren(root);
			if (list.size() == 0) {
				return null;
			}
			//�����а�����С�����˳������
			Collections.sort(list, new Comparator<String>() {
				public int compare(String lhs, String rhs) {
					return getNodeNumber(lhs, Node_NAME).compareTo(getNodeNumber(rhs, Node_NAME));
				}
			});
			
			/**
			 * �������е�Ԫ����ѭ����Ȼ�󹹽������·������ͨ�����·��ȥ��ȡ���
			 */
			for ( String nodeName : list ){
				
				String nodeFullPath = root.concat("/").concat(nodeName);	
				try {
					T node = (T) zkClient.readData(nodeFullPath);
					zkClient.delete(nodeFullPath);
					return node;
				} catch (ZkNoNodeException e) {
					logger.error("",e);
				}
			}
			
			return null;
			
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}

	}

	
	private String getNodeNumber(String str, String nodeName) {
		int index = str.lastIndexOf(nodeName);
		if (index >= 0) {
			index += Node_NAME.length();
			return index <= str.length() ? str.substring(index) : "";
		}
		return str;

	}

}
