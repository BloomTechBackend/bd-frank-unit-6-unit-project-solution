package com.amazon.ata.kindlepublishingservice.activity;

import com.amazon.ata.aws.dynamodb.DynamoDbClientProvider;
import com.amazon.ata.kindlepublishingservice.dao.PublishingStatusDao;
import com.amazon.ata.kindlepublishingservice.dynamodb.models.CatalogItemVersion;
import com.amazon.ata.kindlepublishingservice.dynamodb.models.PublishingStatusItem;
import com.amazon.ata.kindlepublishingservice.enums.PublishingRecordStatus;
import com.amazon.ata.kindlepublishingservice.exceptions.BookNotFoundException;
import com.amazon.ata.kindlepublishingservice.models.Book;
import com.amazon.ata.kindlepublishingservice.models.PublishingStatusRecord;
import com.amazon.ata.kindlepublishingservice.models.requests.GetPublishingStatusRequest;
import com.amazon.ata.kindlepublishingservice.models.response.GetPublishingStatusResponse;
import com.amazon.ata.kindlepublishingservice.publishing.BookPublishRequest;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.lambda.runtime.Context;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static com.amazon.ata.kindlepublishingservice.enums.PublishingRecordStatus.QUEUED;

public class GetPublishingStatusActivity {

    DynamoDBMapper mapper = new DynamoDBMapper(DynamoDbClientProvider.getDynamoDBClient());
    PublishingStatusDao publishingStatusDao = new PublishingStatusDao(mapper);

    @Inject
    public GetPublishingStatusActivity() {}

    //retrieve items
    //if not found throw PublishingStatusNotFound
    //implement the getPublishingStatus
    // needs to return getstatusresponse

    public GetPublishingStatusResponse execute(GetPublishingStatusRequest publishingStatusRequest) {
        List<PublishingStatusRecord> itemRecord = new ArrayList<>();

        PublishingStatusItem item = new PublishingStatusItem();
        item.setPublishingRecordId(publishingStatusRequest.getPublishingRecordId());


        DynamoDBQueryExpression<PublishingStatusItem> queryExpression = new DynamoDBQueryExpression()
                .withHashKeyValues(item)
                .withScanIndexForward(false)
                .withLimit(1);

        List<PublishingStatusItem> results = mapper.query(PublishingStatusItem.class, queryExpression);



        for(int i = 0; i < results.size();i++) {

            PublishingStatusRecord record = PublishingStatusRecord.builder()
                    .withStatus(results.get(i).getStatus().toString())
                    .withBookId(results.get(i).getBookId())
                    .withStatusMessage(results.get(i).getStatusMessage())
                    .build();
            itemRecord.add(record);
        }

        return GetPublishingStatusResponse.builder()
                .withPublishingStatusHistory(itemRecord).build();
    }

}
