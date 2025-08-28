package org.example.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private String itemId;
    private String categoryId;
    private String behavior;
    private Long timestamp;
    
    // 处理后的字段
    private String date;
    private Integer hour;
    private Long behaviorCount;
}