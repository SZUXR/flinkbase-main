package bean;

public class Order {
    public String orderId;
    public String customerId;
    public String productId;
    public int quantity;
    public double price;
    public String orderTime;

    public Order(String orderId, String customerId, String productId, int quantity, double price, String orderTime) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.price = price;
        this.orderTime = orderTime;
    }
    public Order() {}



    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", orderTime='" + orderTime + '\'' +
                '}';
    }


}
