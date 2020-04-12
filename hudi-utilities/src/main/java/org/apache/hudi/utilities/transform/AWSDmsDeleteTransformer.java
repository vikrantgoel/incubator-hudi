/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.payload.AWSDmsAvroPayload;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;

/**
 * A Simple transformer that adds `Op` field with value `I`
 * and `_hoodie_is_deleted` with `true`/`false`,
 * for AWS DMS data, if the fields are not present.
 */
public class AWSDmsDeleteTransformer implements Transformer {

  final String isDeletedField = "_hoodie_is_deleted";

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                            TypedProperties properties) {
    Option<String> opColumnOpt = Option.fromJavaOptional(
            Arrays.stream(rowDataset.columns()).filter(c -> c.equals(AWSDmsAvroPayload.OP_FIELD)).findFirst());

    if (!opColumnOpt.isPresent()) {
      return rowDataset
              .withColumn(AWSDmsAvroPayload.OP_FIELD, lit("I"))
              .withColumn(isDeletedField, lit("false"));
    } else {
      return rowDataset
              .withColumn(isDeletedField,
                      when(col(AWSDmsAvroPayload.OP_FIELD).equalTo("D"), "true").otherwise("false"));
    }
  }
}
