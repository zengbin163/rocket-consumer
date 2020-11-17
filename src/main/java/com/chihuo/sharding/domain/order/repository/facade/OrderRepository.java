package com.chihuo.sharding.domain.order.repository.facade;

import java.util.List;

import com.chihuo.sharding.domain.order.repository.po.OrderPO;

public interface OrderRepository {

	void save(OrderPO orderPO);

	List<OrderPO> findOrderList();
	
	Integer countOrderList();

}
