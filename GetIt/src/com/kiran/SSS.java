/**
 * 
 */
package com.cisco.sts.netprofile.summary;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.springframework.transaction.annotation.Transactional;

import com.cisco.sts.netprofile.beans.vo.TypeCountDevVO;
import com.cisco.sts.netprofile.util.HibernateUtil;
import com.cisco.sts.netprofile.util.NPLibUtil;
import com.cisco.sts.netprofile.util.SummarizerUtil;

/**
 * @author kikanapa
 *
 */
public class FeaturesSummary  implements ComponentSummary  {
	
	private static final Logger log = Logger.getLogger(FeaturesSummary.class.getName());

	/**
	 * 
	 * @param a
	 */
	public static void main(String a[]) {
		FeaturesSummary ss = new FeaturesSummary();
		String custId ="bny2";
		List allGrpIdsList =  NPLibUtil.getAllViewerGroupsForCollectorAsList(custId);
		ss.summarize(custId, null,allGrpIdsList);
	}
	
	
	@Transactional
	public  boolean summarize(String custId,Map<String, List<BigDecimal>>  allViewerGrps,
			List<BigDecimal> grpIdsList ) {
		long startTime = System.currentTimeMillis();
		log.info("Started FeaturesSummary.summarize() method ****");
		Session session = HibernateUtil.getHibernateSession();
		Transaction tx = session.beginTransaction();
		
		BigDecimal cpyKey =  NPLibUtil.getCompanyKeyForCustId(session,custId);
		List<String> collectors = NPLibUtil.getAllCollectorsForCompany(session,cpyKey);
		
		String stmtSelCollSql = null;
		List typeList = new ArrayList();
		if(custId!= null && !custId.isEmpty()) {
			stmtSelCollSql = "select distinct(type_id) from np_tools_summ_master where lower(type_category) = 'feature'";
		}
		Query stmtSelCollQuery = session.createSQLQuery(stmtSelCollSql);
		List stmtSelCollList = stmtSelCollQuery.list();
		typeList = stmtSelCollList;
		
		

		//Deletes the Summary
		deleteSummary(session,stmtSelCollSql,stmtSelCollQuery,custId,typeList,grpIdsList);
		
		
		String collIdSql = "select coll_id from coll where coll_name = :custId and rownum<2";
		Query collIdQuery =  session.createSQLQuery(collIdSql);
		collIdQuery.setParameter("custId", custId);
		List collIdList = collIdQuery.list();
		
		//Update the Summary for Groups
		updateSummaryForGroup(session,stmtSelCollSql,stmtSelCollQuery,custId,typeList,grpIdsList);
		

		/*		#
		#Update the summary for collector
		#*/
		updateSummaryForCollector(session,cpyKey,collectors,stmtSelCollSql,stmtSelCollQuery,custId,typeList,grpIdsList,collIdList);

		try {
			log.info("Processing,please wait...");
			Thread.sleep(100);
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("The Exception is :"+ex);
		} finally {
			tx.commit();
			log.info("***********Committed Transactions, and Closed All Resources****************");
		}
			log.info("Completed FeaturesSummary.summarize() method ****");
			long endTime = System.currentTimeMillis();
			log.info("Total Time Taken to Execute FeaturesSummary.summarize() method  is****"+ ((endTime-startTime)/1000)+ " ms");
			return true;
	}
	

	
	
	/**
	 * This method updated the Summary Information
	 */
	private void deleteSummary(Session session,String stmtSelCollSql,Query stmtSelCollQuery,String custId,
			List typeList,List<BigDecimal> grpIdsList) {
		
		//Delete the Summary
/*		log.info("************FeaturesSummary.deleteSummary() Started************");
		if(custId!= null && !custId.isEmpty()) {
			stmtSelCollSql = "select distinct(type_id) from np_tools_summ_master where lower(type_category) = 'feature'";
		}
		stmtSelCollQuery = session.createSQLQuery(stmtSelCollSql);
		typeList = stmtSelCollQuery.list();		
		
		log.info("passing typeList with size to deleteNPTools* methods "+ typeList.size());*/
		
		SummarizerUtil.deleteNpToolsSummCollector(session,custId,stmtSelCollSql,typeList);
		SummarizerUtil.deleteNpToolsSummGroup(session,grpIdsList,stmtSelCollSql,typeList);
		log.info("************FeaturesSummary.deleteSummary() Completed************");
	}
	
	
	/**
	 * This method updated the Summary Information for Company
	 */
	private void updateSummaryForGroup(Session session,String stmtSelCollSql,Query stmtSelCollQuery,String custId,
			List typeList,List<BigDecimal> grpIdsList) {
		
		//Update the Summary for Group
		log.info("************FeaturesSummary.updateSummaryForGroup() Started************");

		BigDecimal typeCount = new BigDecimal(0);
		BigDecimal typeDevCount = new BigDecimal(0);
		BigDecimal typeCountMpVal  = new BigDecimal(0);
		BigDecimal typeDevCountMapVal = new BigDecimal(0);
		
		stmtSelCollSql = "select  s.type_id, count(child_type),FEAT_TITLE from   coll_managed_device r, np_device_features df,  np_features_master fm, np_group_device g,"
  		 + " np_tools_summ_master s   where r.party_id in (select party_id from coll where coll_name = :custId and rownum<2)  and g.group_id = :grpId	and df.device_id " 
  		 + " = r.managed_device_id   and df.feature_id = fm.feature_id 	and r.managed_device_status_cd = 'ACTIVE'  and g.device_id = r.managed_device_id " 
  		 +" and lower(s.type_category) = 'feature'    and s.type_name = feat_title||'::'||tech_title||'::'||child_type  group by " 
  		 + " child_type,FEAT_TITLE,s.type_id ";
		
		
		stmtSelCollSql = "select  /*+ index(fm NP_FEATURES_MASTER_PRIM) */ s.type_id, count(child_type),FEAT_TITLE from np_device_features df, "
				+" np_features_master fm,np_tools_summ_master s WHERE  exists (select 1 from coll_managed_device r, np_group_device g  where r.party_id "
				+" in(select party_id from coll where coll_name = :custId and rownum<2) and g.group_id = :grpId and df.device_id = r.managed_device_id 	and "
				+" r.managed_device_status_cd = 'ACTIVE' and g.device_id = r.managed_device_id) and df.feature_id = fm.feature_id and " 
				+ " lower(s.type_category) = 'feature' 	and s.type_name = feat_title||'::'||tech_title||'::'||child_type  group by child_type,"
				+" FEAT_TITLE,s.type_id ";
		
		log.info("feature GrpSql********:"+stmtSelCollSql);
		
		stmtSelCollQuery = session.createSQLQuery(stmtSelCollSql);

		
		Map<BigDecimal,TypeCountDevVO> typeCountMap = new ConcurrentHashMap<BigDecimal,TypeCountDevVO>();
		Map<BigDecimal,Map<BigDecimal,TypeCountDevVO>> rsetMap = new ConcurrentHashMap<BigDecimal,Map<BigDecimal,TypeCountDevVO>>();
		Map<BigDecimal,Map<BigDecimal, Map<BigDecimal,TypeCountDevVO>>> grpIdMap = 
				new ConcurrentHashMap<BigDecimal,Map<BigDecimal, Map<BigDecimal,TypeCountDevVO>>>();
		
		
		for(BigDecimal grpId : grpIdsList) {
			stmtSelCollQuery.setParameter("grpId",grpId);
			stmtSelCollQuery.setParameter("custId", custId);
			List<Object[]> stmtSelColl = stmtSelCollQuery.list();
			

				
			for(Object[] stmtSelCollElem:stmtSelColl) {
						typeCount =  (BigDecimal)stmtSelCollElem[1];
						typeDevCount =  (BigDecimal)stmtSelCollElem[1];
			}
			typeCountMpVal = typeCount;
			typeDevCountMapVal = typeDevCount;
			typeCountMap.put(typeCount, new TypeCountDevVO(typeCountMpVal,typeDevCountMapVal));
			rsetMap.put(typeCountMpVal, typeCountMap);
			grpIdMap.put(grpId, rsetMap);
			}
		SummarizerUtil.updateNpToolsSummGroup(session, grpIdMap);
		log.info("************FeaturesSummary.updateSummaryForGroup() Completed************");
	}
	
	
	/**
	 * This method updated the Summary Information for Collector
	 */
	private void updateSummaryForCollector(Session session,BigDecimal cpyKey ,List<String> collectors,String stmtSelCollSql,Query stmtSelCollQuery,String custId,
			List typeList,List<BigDecimal> grpIdsList,List collIdList) {
		//Update the Summary for Group
		log.info("************FeaturesSummary.updateSummaryForCollector() Started************");

		stmtSelCollSql = "select s.type_id, count(child_type),FEAT_TITLE from coll_managed_device r, np_device_features df, np_features_master fm, "
						+" np_tools_summ_master s  WHERE r.coll_id in (select coll_id from coll where coll_name = :custId and rownum<2) and df.device_id = "
						+"	r.managed_device_id and df.feature_id = fm.feature_id  and r.managed_device_status_cd = 'ACTIVE'  and lower(s.type_category)" 
						+" = 'feature'  and s.type_name = feat_title||'::'||tech_title||'::'||child_type  group by child_type,FEAT_TITLE,s.type_id ";
		
		
		stmtSelCollSql = "select  /*+ index(fm NP_FEATURES_MASTER_PRIM) */ s.type_id, count(child_type),FEAT_TITLE 	from np_device_features df,  "
						+" np_features_master fm,np_tools_summ_master s WHERE  exists (select 1 from coll_managed_device r where r.coll_id in( :collIdList)"
						+ "	and r.managed_device_status_cd = 'ACTIVE' and df.device_id = r.managed_device_id) and df.feature_id = fm.feature_id and lower" 
						+ " (s.type_category) = 'feature' 	and s.type_name = feat_title||'::'||tech_title||'::'||child_type group by child_type," 
						+ " FEAT_TITLE,s.type_id ";
		
		log.info("feature Cust Sql********:"+stmtSelCollSql);
		
		stmtSelCollQuery = session.createSQLQuery(stmtSelCollSql);

		
		
		Map<BigDecimal,TypeCountDevVO> typeCountMap = new ConcurrentHashMap<BigDecimal,TypeCountDevVO>();
		Map<BigDecimal,Map<BigDecimal,TypeCountDevVO>> rsetMap = new ConcurrentHashMap<BigDecimal,Map<BigDecimal,TypeCountDevVO>>();
		
		Map<BigDecimal,Map<BigDecimal,TypeCountDevVO>> grpIdMap = 
				new ConcurrentHashMap<BigDecimal,Map<BigDecimal,TypeCountDevVO>>();
		
		Map<BigDecimal,Map<String,Map<String,String>>> featHash = new ConcurrentHashMap<BigDecimal,Map<String,Map<String,String>>> ();
		
		
		Map dummyHash = new ConcurrentHashMap();
		
		for(BigDecimal grpId : grpIdsList) {
		
			stmtSelCollQuery.setParameterList("collIdList", collIdList);
			List<Object[]> stmtSelColl = stmtSelCollQuery.list();
			
			BigDecimal typeCount = new BigDecimal(0);
			BigDecimal typeDevCount = new BigDecimal(0);
			BigDecimal typeCountMpVal  = new BigDecimal(0);
			BigDecimal typeDevCountMapVal = new BigDecimal(0);
				
			for(Object[] stmtSelCollElem:stmtSelColl) {
						typeCount =  (BigDecimal)stmtSelCollElem[1];
						typeDevCount =  (BigDecimal)stmtSelCollElem[1];
			}
			typeCountMpVal = typeCount;
			typeDevCountMapVal = typeDevCount;
			typeCountMap.put(typeCount, new TypeCountDevVO(typeCountMpVal,typeDevCountMapVal));
			rsetMap.put(typeCountMpVal, typeCountMap);
			
			}

		SummarizerUtil.updateNpToolsSummCollector(session, custId, rsetMap, featHash);
		
		SummarizerUtil.updateNpToolsSummCompany(session, cpyKey, collectors, typeList);
		
		log.info("************FeaturesSummary.updateSummaryForGroup() Completed************");

	}

}
