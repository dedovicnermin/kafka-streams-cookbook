package tech.nermindedovic.kafkastreamscookbook.config.pojos;


import lombok.*;

import java.util.Date;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class WordCount {
    private String word;
    private int count;
    private Date start;
    private Date end;

}
