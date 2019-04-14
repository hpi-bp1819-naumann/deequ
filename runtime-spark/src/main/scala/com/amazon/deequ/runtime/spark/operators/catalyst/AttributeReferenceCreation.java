/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.runtime.spark.operators.catalyst;

import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.AttributeReference$;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression$;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.Metadata$;

import java.lang.reflect.Method;

public class AttributeReferenceCreation {

    private AttributeReferenceCreation() { }

    /**
        Allows us to invoke the apply method on
        org.apache.spark.sql.catalyst.expressions.AttributeReference which has a non-compatible
        signature in different versions of Spark 2.x. Therefore we need to invoke it via reflection
        depending on which version of Spark we run.

        SPARK 2.3:

            case class AttributeReference(
                name: String,
                dataType: DataType,
                nullable: Boolean = true,
                override val metadata: Metadata = Metadata.empty)(
                val exprId: ExprId = NamedExpression.newExprId,
                val qualifier: Option[String] = None)

        SPARK 2.2:

            case class AttributeReference(
                name: String,
                dataType: DataType,
                nullable: Boolean = true,
                override val metadata: Metadata = Metadata.empty)(
                val exprId: ExprId = NamedExpression.newExprId,
                val qualifier: Option[String] = None,
                override val isGenerated: java.lang.Boolean = false)
    */
    public static AttributeReference createSafe(String name) throws IllegalStateException {
        try {

            Class clazz = AttributeReference$.class;
            Method apply = null;

            for (Method method : clazz.getMethods()) {
                if (method.getName().equals("apply")) {
                    apply = method;
                    break;
                }
            }

            if (apply == null) {
                throw new IllegalStateException("Unable to find apply method!");
            }

            LongType dataType = LongType$.MODULE$.asNullable();
            Metadata emptyMetadata = Metadata$.MODULE$.empty();
            scala.Option none = scala.Option.apply(null);
            ExprId exprId = NamedExpression$.MODULE$.newExprId();

            Object companion = AttributeReference$.MODULE$;

            if (apply.getParameterCount() == 7) {
                // Spark 2.2
                return (AttributeReference) apply.invoke(companion, name, dataType, true,
                        emptyMetadata, exprId, none, false);
            } else {
                // Spark 2.3
                return (AttributeReference) apply.invoke(companion, name, dataType, true,
                        emptyMetadata, exprId, none);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
