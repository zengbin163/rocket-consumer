package com.chihuo.sharding.domain.order.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.chihuo.sharding.domain.order.entity.Order;
import com.chihuo.sharding.domain.order.repository.facade.OrderRepository;
import com.chihuo.sharding.domain.order.repository.po.OrderPO;

@Service
public class OrderDomainService {

	@Autowired
    private OrderRepository orderRepository;
    @Autowired
    private OrderFactory factory;

	@Transactional(rollbackFor = Exception.class)
    public Long saveOrder(Order order) {
    	this.orderRepository.save(this.factory.createOrderPO(order));
    	return 0L;
    }

	public List<Order> findOrderList(Integer current, Integer size) {
		//PageHelper.startPage(current, size);
		List<OrderPO> poList = this.orderRepository.findOrderList();
		List<Order> orderList = new ArrayList<>();
		for(OrderPO po : poList) {
			orderList.add(this.factory.createOrder(po));
		}
		return orderList;
	}
	
	public Integer countOrderList() {
		return this.orderRepository.countOrderList();
	}
	
	
}
