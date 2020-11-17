package com.chihuo.sharding.domain.order.repository.persistence;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.chihuo.sharding.domain.order.repository.facade.OrderRepository;
import com.chihuo.sharding.domain.order.repository.mapper.OrderDao;
import com.chihuo.sharding.domain.order.repository.po.OrderPO;

@Repository
public class OrderRepositoryImpl implements OrderRepository {

	@Autowired
	private OrderDao orderDao;
	
	@Override
	public void save(OrderPO orderPO) {
		this.orderDao.save(orderPO);
	}

	@Override
	public List<OrderPO> findOrderList() {
		return this.orderDao.findOrderList();
	}

	@Override
	public Integer countOrderList() {
		return this.orderDao.countOrderList();
	}

}
