package cn.bcos.browser.service;

import static cn.bcos.browser.util.Constants.ADMIN_NODE_INFO;
import static cn.bcos.browser.util.Constants.CODE_MONI_10001;
import static cn.bcos.browser.util.Constants.CODE_MONI_10004;
import static cn.bcos.browser.util.Constants.ETH_BLOCK_NUMBER;
import static cn.bcos.browser.util.Constants.ETH_GET_BLOCK_BY_NUMBER;
import static cn.bcos.browser.util.Constants.ETH_GET_TRANSACTION_RECEIPT;
import static cn.bcos.browser.util.Constants.MSG_MONI_10001;
import static cn.bcos.browser.util.Constants.MSG_MONI_10004;
import static cn.bcos.browser.util.LogUtils.getMonitorLogger;

import java.lang.Thread.UncaughtExceptionHandler;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.googlecode.jsonrpc4j.JsonRpcHttpClient;

import cn.bcos.browser.dao.GovernServiceDAO;
import cn.bcos.browser.dto.BlockInfoDTO;
import cn.bcos.browser.dto.PeerRpcDTO;
import cn.bcos.browser.dto.ReceiptInfoDTO;
import cn.bcos.browser.dto.TransactionInfoDTO;

@Component
public class SyncService {
	private Logger logger = LoggerFactory.getLogger(SyncService.class);
	@Autowired
	private ThreadPoolTaskExecutor threadPool;
	
	private static LinkedBlockingDeque<BlockInfoDTO> blockQueue = new LinkedBlockingDeque<>(10000);
	private static LinkedBlockingDeque<TransactionInfoDTO> transQueue = new LinkedBlockingDeque<>(100000);
	private static LinkedBlockingDeque<ReceiptInfoDTO> receiptQueue = new LinkedBlockingDeque<>(100000);

	/**
	 * start queue
	 */
	public void startQueue(){
		threadPool.execute(new putQueue());
		threadPool.execute(new popBlockQueue());
		threadPool.execute(new popTransQueue());
		threadPool.execute(new popReceiptQueue());
	}
	/**
	 * putQueue
	 * @author jsmen
	 *
	 */
	class putQueue implements Runnable{
		@Override
		public void run() {
			handleBlockInfo();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("putQueue sleep Exception!");
			}
			
		}
		/**
		 * handle blockInfo
		 */
		public void handleBlockInfo() {
			long startTime = System.currentTimeMillis();
			String lastBlock = (String) getInfoByMethod(ETH_BLOCK_NUMBER, null);
			if(lastBlock==null){
				return;
			}
			int blockHeight = Integer.parseInt(lastBlock.substring(2), 16);
			logger.debug("###handleBlockInfo the latest blockHeight：{}###", blockHeight);
			long startTime1 = System.currentTimeMillis();
			int currentHeight;
			if(blockQueue.isEmpty()){
				currentHeight = governServiceDAO.selectBlockHeigth();
			}else{
				currentHeight = blockQueue.getLast().getNumber();
			}
			long endtTime1 = System.currentTimeMillis();
			getMonitorLogger().info(CODE_MONI_10004, endtTime1 - startTime1, MSG_MONI_10004);
			logger.debug("###handleBlockInfo current blockHeight：{}###", currentHeight);
			
			long endtTime = 0;
			if (blockHeight == 0 || blockHeight == currentHeight) {
				endtTime = System.currentTimeMillis();
				getMonitorLogger().info(CODE_MONI_10001, endtTime - startTime, MSG_MONI_10001);
				return;
			} else {
				for (int i = currentHeight + 1; i <= blockHeight; i++) {
					if (i == 1) {
						handleBlockInfo(0);
						handleBlockInfo(1);
					} else {
						handleBlockInfo(i);
					}
				}
				governServiceDAO.insertTxnByDayInfo();
			}
			endtTime = System.currentTimeMillis();
			getMonitorLogger().info(CODE_MONI_10001, endtTime - startTime, MSG_MONI_10001);		
		}
		/**
		 * handle block info
		 * 
		 * @param blockHeight
		 */
		public void handleBlockInfo(int blockHeight){
			Object[] params = new Object[] { String.valueOf(blockHeight), true };
			Object blockInfo = getInfoByMethod(ETH_GET_BLOCK_BY_NUMBER, params);
			if(blockInfo==null){
				return;
			}
			JSONObject json = JSONObject.parseObject(JSON.toJSONString(blockInfo));
			logger.debug("###blockInfo：{}###", json);

			Map<String, Object> map = handleTransInfo(json);
			BigDecimal gasPriceTotal = new BigDecimal(map.get("gasPriceTotal").toString());
			BigDecimal transCount = new BigDecimal(map.get("transCount").toString());

			blockInfoDTO = new BlockInfoDTO();
			blockInfoDTO.setPk_hash(json.getString("hash"));
			blockInfoDTO.setNumber(Integer.parseInt(json.getString("number").substring(2), 16));
			blockInfoDTO.setParentHash(json.getString("parentHash"));
			//miner--minerNodeId
			blockInfoDTO.setMiner(null!=json.getString("minerNodeId") ?json.getString("minerNodeId"):"0x0");
			blockInfoDTO.setGenIndex(Integer.parseInt(json.getString("genIndex").substring(2), 16));
			blockInfoDTO.setSize(Integer.parseInt(json.getString("size").substring(2), 16));
			blockInfoDTO.setGasLimit(Long.parseLong(json.getString("gasLimit").substring(2), 16));
			blockInfoDTO.setGasUsed(Long.parseLong(json.getString("gasUsed").substring(2), 16));
			if (new BigDecimal("0").equals(gasPriceTotal)) {
				blockInfoDTO.setAvgGasPrice(new BigDecimal("0"));
			} else {
				blockInfoDTO.setAvgGasPrice(gasPriceTotal.divide(transCount, 8, BigDecimal.ROUND_HALF_UP));
			}
			if ("0".equals(json.getString("timestamp").substring(2))) {
				blockInfoDTO.setTimestamp(null);
			} else {
				blockInfoDTO.setTimestamp(new Timestamp(Long.parseLong(json.getString("timestamp").substring(2), 16)));
			}
			blockInfoDTO.setTxn(Long.parseLong(map.get("transCount").toString()));
			blockInfoDTO.setExtraData(json.getString("extraData"));
			blockInfoDTO.setDetailInfo(json.toString());
			try {
				blockQueue.put(blockInfoDTO);
			} catch (InterruptedException e) {
				logger.error("putBlockQueue Exception!");
			}
			//governServiceDAO.insertBlockInfo(blockInfoDTO);
		}

		/**
		 * handle transaction info
		 * 
		 * @param json
		 * @return Map<String, Object>
		 */
		public Map<String, Object> handleTransInfo(JSONObject json) {
			Map<String, Object> map = new HashMap<String, Object>();
			long gasPriceTotal = 0;
			JSONArray jsonArr = json.getJSONArray("transactions");
			logger.debug("###transactions：{}###", jsonArr);
			long jsonSize = jsonArr.size();
			for (int j = 0; j < jsonSize; j++) {
				JSONObject jsonTrans = jsonArr.getJSONObject(j);
				gasPriceTotal = gasPriceTotal + Long.parseLong(jsonTrans.getString("gasPrice").substring(2), 16);
				transactionInfoDTO = new TransactionInfoDTO();
				transactionInfoDTO.setPk_hash(jsonTrans.getString("hash"));
				transactionInfoDTO.setBlockHash(jsonTrans.getString("blockHash"));
				transactionInfoDTO.setBlockNumber(Integer.parseInt(jsonTrans.getString("blockNumber").substring(2), 16));
				if ("0".equals(json.getString("timestamp").substring(2))) {
					transactionInfoDTO.setBlockTimestamp(null);
				} else {
					transactionInfoDTO.setBlockTimestamp(new Timestamp(Long.parseLong(json.getString("timestamp").substring(2), 16)));
				}
				transactionInfoDTO.setBlockGasLimit(Long.parseLong(json.getString("gasLimit").substring(2), 16));
				transactionInfoDTO.setTransactionIndex(Long.parseLong(jsonTrans.getString("transactionIndex").substring(2), 16));
				transactionInfoDTO.setTransactionFrom(jsonTrans.getString("from"));
				transactionInfoDTO.setTransactionTo(jsonTrans.getString("to"));
				transactionInfoDTO.setGas(Long.parseLong(jsonTrans.getString("gas").substring(2), 16));
				transactionInfoDTO.setGasPrice(BigDecimal.valueOf(Long.parseLong(jsonTrans.getString("gasPrice").substring(2), 16)));
				if (j == 0) {
					transactionInfoDTO.setCumulativeGas(Long.parseLong(jsonTrans.getString("gas").substring(2), 16));
				} else {
					long cumulativeGas = 0;
					for (int k = 0; k <= j; k++) {
						JSONObject jsonObj = jsonArr.getJSONObject(k);
						cumulativeGas = cumulativeGas + Long.parseLong(jsonObj.getString("gas").substring(2), 16);
					}
					transactionInfoDTO.setCumulativeGas(cumulativeGas);
				}
				transactionInfoDTO.setRandomId(jsonTrans.getString("randomId"));
				transactionInfoDTO.setInputText(jsonTrans.getString("input"));
				
				transactionInfoDTO.setContractName(null!=jsonTrans.getJSONObject("operation")?jsonTrans.getJSONObject("operation").getString("contractName"):"");
				transactionInfoDTO.setVersion(null!=jsonTrans.getJSONObject("operation")?jsonTrans.getJSONObject("operation").getString("version"):"");
				transactionInfoDTO.setMethod(null!=jsonTrans.getJSONObject("operation")?jsonTrans.getJSONObject("operation").getString("method"):"");
				transactionInfoDTO.setParams(null!=jsonTrans.getJSONObject("operation")?jsonTrans.getJSONObject("operation").getString("params"):"");
				//governServiceDAO.insertTransactionInfo(transactionInfoDTO);
				try {
					transQueue.put(transactionInfoDTO);
				} catch (InterruptedException e) {
					logger.error("putTransQueue Exception!");
				}			
				handleTransReceiptInfo(jsonTrans.getString("hash"));
			}
			map.put("transCount", jsonSize);
			map.put("gasPriceTotal", gasPriceTotal);
			return map;
		}
		
		/**
		 * handle transaction receipt info
		 */
		public void handleTransReceiptInfo(String hash){
			Object[] params = new Object[] { hash };
			Object receiptInfo = getInfoByMethod(ETH_GET_TRANSACTION_RECEIPT, params);
			if(receiptInfo==null){
				return;
			}
			JSONObject receiptJson = JSONObject.parseObject(JSON.toJSONString(receiptInfo));
			logger.debug("###receipt：{}###", receiptJson);
			receiptInfoDTO = new ReceiptInfoDTO();
			receiptInfoDTO.setPk_hash(receiptJson.getString("transactionHash"));
			receiptInfoDTO.setBlockHash(receiptJson.getString("blockHash"));
			receiptInfoDTO.setBlockNumber(Integer.parseInt(receiptJson.getString("blockNumber")));
			receiptInfoDTO.setContractAddress(receiptJson.getString("contractAddress"));
			receiptInfoDTO.setTransactionIndex(Long.parseLong(receiptJson.getString("transactionIndex")));
			receiptInfoDTO.setGasUsed(Long.parseLong(receiptJson.getString("gasUsed").substring(2), 16));
			receiptInfoDTO.setCumulativeGasUsed(Long.parseLong(receiptJson.getString("cumulativeGasUsed").substring(2), 16));
			receiptInfoDTO.setLogs(receiptJson.getString("logs"));
			receiptInfoDTO.setDetailInfo(receiptJson.toString());
			//governServiceDAO.insertReceiptInfo(receiptInfoDTO);
			try {
				receiptQueue.put(receiptInfoDTO);
				logger.debug("receiptQueue isempty："+receiptQueue.isEmpty());
			} catch (InterruptedException e) {
				logger.error("putReceiptQueue Exception!");
			}
		}
	}
		
	/**
	 * insert block
	 * @author jsmen
	 *
	 */
	class popBlockQueue implements Runnable{
		@Override
		public void run() {
			BlockInfoDTO block = null;
			while(true){
				try {
					block = blockQueue.take();
					governServiceDAO.insertBlockInfo(block);
					Thread.sleep(1);
				} catch (InterruptedException e) {
					logger.error("popBlockQueue Exception!"+e.getMessage());
				} catch (Exception e) {
					logger.error("popBlockQueue Exception!"+e.getMessage());
				}
			}
		}		
	}
	/**
	 * insert trans
	 * @author jsmen
	 *
	 */
	class popTransQueue implements Runnable{

		@Override
		public void run() {
			TransactionInfoDTO trans = null;
			while(true){
				try {
					trans = transQueue.take();
					governServiceDAO.insertTransactionInfo(trans);
					Thread.sleep(1);
				} catch (InterruptedException e) {
					logger.error("popTransQueue Exception!"+e.getMessage());
				} catch (Exception e) {
					logger.error("popTransQueue Exception!"+e.getMessage());
				}
			}
		}
		
	}
	/**
	 * insert receipt
	 * @author jsmen
	 *
	 */
	class popReceiptQueue implements Runnable{
		@Override
		public void run() {
			ReceiptInfoDTO receipt = null;
			while(true){
				try {
					receipt = receiptQueue.take();
					governServiceDAO.insertReceiptInfo(receipt);
					Thread.sleep(1);
				} catch (InterruptedException e) {
					logger.error("popReceiptQueue Exception!"+e.getMessage());
				} catch (Exception e) {
					logger.error("popReceiptQueue Exception!"+e.getMessage());
				}

				
			}
		}		
	}
	/**
	 * catch thread error and restart thread
	 * @author jsmen
	 *
	 */
	class myCatchError implements UncaughtExceptionHandler{
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			logger.error(t + "thread run exception:"+e.getMessage());
			threadPool.execute(t);		
		}
		
	}
	/**
	 * get node rpc info
	 * 
	 * @param methodName
	 * @param params
	 * @return Object
	 */
	public Object getInfoByMethod(String methodName, Object[] params)  {
		Object object=null;
		try {
			JsonRpcHttpClient client = null;
			List<PeerRpcDTO> list=governServiceDAO.selectPeerRpc();
			for (int i=0;i<list.size();i++){
				client = new JsonRpcHttpClient(new URL("http://"+list.get(i).getIp()+":"+list.get(i).getRpcPort()));
				try {
					Object currentNodeInfo = client.invoke(ADMIN_NODE_INFO, null, Object.class);
					JSONObject currentJson = JSONObject.parseObject(JSON.toJSONString(currentNodeInfo));
					object = client.invoke(methodName, params, Object.class);
					if(null !=currentJson){
						break;
					}
				} catch (Exception e) {
					logger.error(list.get(i).getIp()+":"+list.get(i).getRpcPort()+"node die！");
				}
			}
			return object;
		} catch (Throwable e) {
			logger.error("rpc Exception!!!");
			return object;
		}
	}
	
	@Autowired
	private GovernServiceDAO governServiceDAO;
	private BlockInfoDTO blockInfoDTO;
	private TransactionInfoDTO transactionInfoDTO;
	private ReceiptInfoDTO receiptInfoDTO;
}
