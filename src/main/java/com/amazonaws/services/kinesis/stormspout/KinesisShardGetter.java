/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.stormspout;

import com.amazonaws.AmazonClientException;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.stormspout.exceptions.InvalidSeekPositionException;
import com.amazonaws.services.kinesis.stormspout.exceptions.KinesisSpoutException;
import com.amazonaws.services.kinesis.stormspout.utils.InfiniteConstantBackoffRetry;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Fetches data from a Kinesis shard.
 */
class KinesisShardGetter implements IShardGetter {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardGetter.class);

    private static final long BACKOFF_MILLIS = 500L;

    private final String streamName;
    private final String shardId;
    private int firstCall;
    private int maxRecords=100;
    //private final AmazonKinesisClient kinesisClient;

    AmazonKinesisAsyncClient kinesisClient;
    final ConcurrentLinkedQueue<Record> recordsQueue=new ConcurrentLinkedQueue<>();

    private String shardIterator;
    private ShardPosition positionInShard;

    /**
     * @param streamName Name of the Kinesis stream
     * @param shardId Fetch data from this shard
     * @param kinesisClient Kinesis client to use when making requests.
     */
    KinesisShardGetter(final String streamName, final String shardId, final AmazonKinesisAsyncClient kinesisClient, final int maxRecords) {
        this.streamName = streamName;
        this.shardId = shardId;
        this.kinesisClient = kinesisClient;
        this.shardIterator = "";
        this.positionInShard = ShardPosition.end();
        this.firstCall = 0;
    }

    public void readRecords(final int maxRecordsPerCall) throws AmazonClientException, ResourceNotFoundException, InvalidArgumentException {

        if(!shardIterator.equals(""))    {

            final GetRecordsRequest request = new GetRecordsRequest();
            request.setShardIterator(shardIterator);
            request.setLimit(maxRecordsPerCall);
            kinesisClient.getRecordsAsync(request, new AsyncHandler<GetRecordsRequest, GetRecordsResult>() {

                @Override
                public void onSuccess(GetRecordsRequest request, GetRecordsResult result) {
                    if(result.getRecords()!=null && result.getRecords().size()>0){
                        recordsQueue.addAll(result.getRecords());
                        for (Record rec : result.getRecords()) {
                            positionInShard = ShardPosition.afterSequenceNumber(rec.getSequenceNumber());
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(this + " async fetched " + result.getRecords().size() + " records from Kinesis (requested "
                                    + maxRecordsPerCall + ").");
                        }

                        shardIterator = result.getNextShardIterator();
                    }
                    else{
                        try {
                            LOG.debug("Sleeping for 1 second as got 0 records in shard "+shardId);
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                    readRecords(maxRecordsPerCall);
                }

                @Override
                public void onError(Exception e) {
                    e.printStackTrace();
                    LOG.debug("Exception fetching "+shardId);
                    if(e instanceof ExpiredIteratorException){
                        LOG.debug("Expired shard iterator, seeking to last known position.");
                        try {
                            seek(positionInShard);
                        } catch (InvalidSeekPositionException e1) {
                            LOG.error("Could not seek to last known position after iterator expired.");
                            throw new KinesisSpoutException(e1);
                        }
                        request.setShardIterator(shardIterator);
                    }
                    try {
                        LOG.debug("Sleeping for 1 second as got Exception in shard "+shardId);
                        Thread.sleep(1000);
                        readRecords(maxRecordsPerCall);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
            });
        }
        else{
            try {
                LOG.debug("Sleeping for 10 second as got empty shard iterator for shard "+shardId);
                Thread.sleep(10000);
                firstCall=0;
                seek(positionInShard);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }  catch (InvalidSeekPositionException e1) {
                LOG.error("Could not seek to last known position after iterator expired.");
                throw new KinesisSpoutException(e1);
            }
        }
    }

    @Override
    public Records getNext(int maxNumberOfRecords)
            throws AmazonClientException, ResourceNotFoundException, InvalidArgumentException {
        final ImmutableList.Builder<Record> records = new ImmutableList.Builder<>();
        if (shardIterator == null) {
            LOG.debug(this + " Null shardIterator for " + shardId + ". This can happen if shard is closed.");
            return Records.empty(true);
        }
        LOG.debug("Queue had "+recordsQueue.size()+" records for shard "+shardId);
        for (int i = 0; i < maxNumberOfRecords; i++) {
            if (!recordsQueue.isEmpty()) {
                Record rec = recordsQueue.poll();
                records.add(rec);
            } else { break; }
        }
        LOG.debug("Returning Queue has "+recordsQueue.size()+" records for shard "+shardId);
        return new Records(records.build(), shardIterator == null);
    }

    @Override
    public void seek(ShardPosition position)
        throws AmazonClientException, ResourceNotFoundException, InvalidSeekPositionException {
        LOG.debug("Seeking to " + position);

        ShardIteratorType iteratorType;
        String seqNum = null;
        Date timestamp=null;
        switch (position.getPosition()) {
            case TRIM_HORIZON:
                iteratorType = ShardIteratorType.TRIM_HORIZON;
                break;
            case LATEST:
                iteratorType = ShardIteratorType.LATEST;
                break;
            case AT_SEQUENCE_NUMBER:
                iteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER;
                seqNum = position.getSequenceNum();
                break;
            case AFTER_SEQUENCE_NUMBER:
                iteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
                seqNum = position.getSequenceNum();
                break;
            case AT_TIMESTAMP:
                iteratorType = ShardIteratorType.AT_TIMESTAMP;
                timestamp = position.getTimeStamp();
                break;
            default:
                LOG.error("Invalid seek position " + position);
                throw new InvalidSeekPositionException(position);
        }

        try {
            shardIterator = seek(iteratorType, seqNum, timestamp);
            if(firstCall==0){
                LOG.debug("Call If First seek Only");
                readRecords(maxRecords);
                firstCall++;
            }
        } catch (InvalidArgumentException e) {
            LOG.error("Error occured while seeking, cannot seek to " + position + ".", e);
            throw new InvalidSeekPositionException(position);
        } catch (Exception e) {
            LOG.error("Irrecoverable exception, rethrowing.", e);
            throw e;
        }

        positionInShard = position;
    }

    @Override
    public String getAssociatedShard() {
        return shardId;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("shardId", shardId).toString();
    }

    private String seek(final ShardIteratorType iteratorType, final String seqNum, final Date timestamp)
        throws AmazonClientException, ResourceNotFoundException, InvalidArgumentException {
        final GetShardIteratorRequest request = new GetShardIteratorRequest();

        request.setStreamName(streamName);
        request.setShardId(shardId);
        request.setShardIteratorType(iteratorType);

        // SeqNum is only set on {AT, AFTER}_SEQUENCE_NUMBER, so this is safe.
        if (seqNum != null) {
            request.setStartingSequenceNumber(seqNum);
        }

        if (timestamp != null) {
            request.setTimestamp(timestamp);
        }

        return new InfiniteConstantBackoffRetry<String>(BACKOFF_MILLIS,
                AmazonClientException.class,
                new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        LOG.debug("In Kinesis Shard Getter  "+request);
                        GetShardIteratorResult result = kinesisClient.getShardIterator(request);
                        return result.getShardIterator();
                    }
                }).call();
    }

    private GetRecordsResult safeGetRecords(final GetRecordsRequest request)
        throws AmazonClientException, ResourceNotFoundException, InvalidArgumentException {
        while (true) {
            try {
                return kinesisClient.getRecords(request);
            } catch (ExpiredIteratorException e) {
                LOG.info("Expired shard iterator, seeking to last known position.");
                try {
                    seek(positionInShard);
                } catch (InvalidSeekPositionException e1) {
                    LOG.error("Could not seek to last known position after iterator expired.");
                    throw new KinesisSpoutException(e1);
                }
                request.setShardIterator(shardIterator);
            }
        }
    }
}
