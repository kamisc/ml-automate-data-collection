package com.sewerynkamil.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class SaleTransaction {
    private String uuid;
    private String timestamp;
    private String type;
    private String size;
    private String price;
    private String offer;
    private String discount;
    private long userId;
    private Country country;
    private String city;

    @JsonProperty("tran_id")
    @JsonAlias("transaction_id")
    public String getUuid() {
        return uuid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    @JsonProperty("coffee_type")
    public String getType() {
        return type;
    }

    @JsonProperty("coffee_size")
    public String getSize() {
        return size;
    }

    public String getPrice() {
        return price;
    }

    public String getOffer() {
        return offer;
    }

    public String getDiscount() {
        return discount;
    }

    @JsonProperty("userid")
    public long getUserId() {
        return userId;
    }

    @JsonIgnore
    public Country getCountry() {
        return country;
    }

    @JsonIgnore
    public String getCity() {
        return city;
    }

    public static final class SaleTransactionBuilder {
        private String uuid;
        private String timestamp;
        private String type;
        private String size;
        private String price;
        private String offer;
        private String discount;
        private Long userId;
        private Country country;
        private String city;

        public SaleTransactionBuilder uuid(final String uuid) {
            this.uuid = uuid;
            return this;
        }

        public SaleTransactionBuilder timestamp(final String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public SaleTransactionBuilder type(final String type) {
            this.type = type;
            return this;
        }

        public SaleTransactionBuilder size(final String size) {
            this.size = size;
            return this;
        }

        public SaleTransactionBuilder price(final String price) {
            this.price = price;
            return this;
        }

        public SaleTransactionBuilder offer(final String offer) {
            this.offer = offer;
            return this;
        }

        public SaleTransactionBuilder discount(final String discount) {
            this.discount = discount;
            return this;
        }

        public SaleTransactionBuilder userId(final Long userId) {
            this.userId = userId;
            return this;
        }

        public SaleTransactionBuilder country(final Country country) {
            this.country = country;
            return this;
        }

        public SaleTransactionBuilder city(final String city) {
            this.city = city;
            return this;
        }

        public SaleTransaction build() {
            SaleTransaction saleTransaction = new SaleTransaction();
            saleTransaction.uuid = this.uuid;
            saleTransaction.timestamp = this.timestamp;
            saleTransaction.type = this.type;
            saleTransaction.size = this.size;
            saleTransaction.price = this.price;
            saleTransaction.offer = this.offer;
            saleTransaction.discount = this.discount;
            saleTransaction.userId = this.userId;
            saleTransaction.country = this.country;
            saleTransaction.city = this.city;

            return saleTransaction;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SaleTransaction that = (SaleTransaction) o;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    @Override
    public String toString() {
        return "SaleTransaction{" +
                "uuid='" + uuid + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", type='" + type + '\'' +
                ", size='" + size + '\'' +
                ", price='" + price + '\'' +
                ", offer='" + offer + '\'' +
                ", discount='" + discount + '\'' +
                ", userId=" + userId +
                ", country=" + country.name() +
                ", city='" + city + '\'' +
                '}';
    }

    public enum Country {
        UK, JAPAN, ITALY, CANADA
    }
}
