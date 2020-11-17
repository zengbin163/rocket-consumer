package com.chihuo.sharding.domain.order.repository.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;

import com.chihuo.sharding.domain.order.repository.po.OrderPO;

public interface OrderDao {
	
	void save(@Param("orderPO") OrderPO orderPO);
	
	List<OrderPO> findOrderList();
	
	Integer countOrderList();
	
}