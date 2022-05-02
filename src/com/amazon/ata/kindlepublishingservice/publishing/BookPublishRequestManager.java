package com.amazon.ata.kindlepublishingservice.publishing;

import com.amazon.ata.aws.dynamodb.DynamoDbClientProvider;
import com.amazon.ata.kindlepublishingservice.dynamodb.models.CatalogItemVersion;
import com.amazon.ata.kindlepublishingservice.exceptions.BookNotFoundException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BookPublishRequestManager {

    DynamoDBMapper mapper = new DynamoDBMapper(DynamoDbClientProvider.getDynamoDBClient());
    Queue<BookPublishRequest> bookRequest = new LinkedList<>();

    @Inject
    public BookPublishRequestManager(){}

    public void addBookPublishRequest(BookPublishRequest bookPublishRequest){
        CatalogItemVersion book = new CatalogItemVersion();

        if(bookPublishRequest.getBookId() != null){
            book.setBookId(bookPublishRequest.getBookId());

            DynamoDBQueryExpression<CatalogItemVersion> queryExpression = new DynamoDBQueryExpression()
                    .withHashKeyValues(book)
                    .withScanIndexForward(false)
                    .withLimit(1);

            List<CatalogItemVersion> result = mapper.query(CatalogItemVersion.class, queryExpression);
            if(result.isEmpty()){
                throw new BookNotFoundException(String.format("NO book"));
            } else {
                bookRequest.add(bookPublishRequest);
            }
        }
    }

    public BookPublishRequest getBookPublishRequestToProcess(){
        while(!bookRequest.isEmpty()){
            return bookRequest.remove();
        }
        return null;
    }
}