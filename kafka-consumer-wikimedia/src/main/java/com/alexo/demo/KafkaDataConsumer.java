package com.alexo.demo;

import com.alexo.demo.entity.WikimediaData;
import com.alexo.demo.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDataConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataConsumer.class);

    private WikimediaDataRepository wikimediaDataRepository;

    public KafkaDataConsumer(WikimediaDataRepository wikimediaDataRepository) {
        this.wikimediaDataRepository = wikimediaDataRepository;
    }

    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consume(String eventMessage) {
        LOGGER.info("consume eventMessage: {}", eventMessage);
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);
        wikimediaDataRepository.save(wikimediaData);
    }

}
