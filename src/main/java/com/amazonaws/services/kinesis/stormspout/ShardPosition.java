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

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.io.Serializable;
import java.util.Date;

/**
 * Position into a Kinesis shard.
 */
public class ShardPosition implements Serializable {
    private static final long serialVersionUID = -2629693156437171114L;

    private final Position pos;
    private final String sequenceNum;
    private final Date timeStamp;

    /**
     * @return a new ShardPosition to fetch the first available record in the shard.
     */
    public static ShardPosition trimHorizon() {
        return new ShardPosition(Position.TRIM_HORIZON, null,null);
    }

    /**
     * @return a new ShardPosition to fetch new data in the shard
     */
    public static ShardPosition end() {
        return new ShardPosition(Position.LATEST, null,null);
    }

    /**
     * @param sequenceNum sequence number to start at.
     * @return a new ShardPosition starting AT_SEQUENCE_NUMBER sequenceNum.
     */
    public static ShardPosition atSequenceNumber(final String sequenceNum) {
        return new ShardPosition(Position.AT_SEQUENCE_NUMBER, sequenceNum,null);
    }

    /**
     * @param timeStamp timeStamp number to start at.
     * @return a new ShardPosition starting AT_TIMESTAMP sequenceNum.
     */
    public static ShardPosition atTimestamp(final Date timeStamp) {
        return new ShardPosition(Position.AT_TIMESTAMP, null,timeStamp);
    }

    /**
     * @param sequenceNum sequence number to start after.
     * @return a new ShardPosition starting AFTER_SEQUENCE_NUMBER sequenceNum.
     */
    public static ShardPosition afterSequenceNumber(final String sequenceNum) {
        return new ShardPosition(Position.AFTER_SEQUENCE_NUMBER, sequenceNum,null);
    }

    private ShardPosition(final Position pos, final String sequenceNum,final Date timeStamp) {
        this.pos = pos;
        this.sequenceNum = sequenceNum;
        this.timeStamp = timeStamp;
    }

    /**
     * Depending on the return value, it might also be necessary to read the sequence number.
     * 
     * @return the position to seek to.
     */
    public Position getPosition() {
        return pos;
    }

    /**
     * Optional argument to ShardPosition, only used with {AT, AFTER}_SEQUENCE_NUMBER.
     * 
     * @return the sequence number to seek at/after.
     */
    public String getSequenceNum() {
        return sequenceNum;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    /**
     * Position in shard.
     */
    public static enum Position {
        /** TIME_ZERO or TRIM_HORIZON. */
        TRIM_HORIZON,
        /** AT_SEQUENCE_NUMBER. */
        AT_SEQUENCE_NUMBER,
        /** AFTER_SEQUENCE_NUMBER. */
        AFTER_SEQUENCE_NUMBER,
        /** LATEST. */
        LATEST,
        /** AT_TIMESTAMP. */
        AT_TIMESTAMP;
    }
}
