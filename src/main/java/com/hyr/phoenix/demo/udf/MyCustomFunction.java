package com.hyr.phoenix.demo.udf;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.List;

/*******************************************************************************
 * @date 2018-08-08 下午 3:27
 * @author: huangyueran
 * @Description: phoenix 自定义function UDF
 ******************************************************************************/
@FunctionParseNode.BuiltInFunction(name = MyCustomFunction.NAME, args = {@FunctionParseNode.Argument(allowedTypes = {PVarchar.class})})
public class MyCustomFunction extends ScalarFunction {
    public static final String NAME = "CHARFORMAT";

    public static final PDataType TYPE = PVarchar.INSTANCE;

    public MyCustomFunction() {
    }

    public MyCustomFunction(List<Expression> children) {
        super(children);
    }

    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression fieldExp = (Expression) getChildren().get(0);

        if (!fieldExp.evaluate(tuple, ptr)) {
            return false;
        }
        String field = (String) PVarchar.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());

        char[] chars = field.toCharArray();
        StringBuilder sb = new StringBuilder();
        for (char c : chars) {
            sb.append(c + "\t\t\t");
        }
        System.out.println(sb.toString());
        ptr.set(PVarchar.INSTANCE.toBytes("true"));

        return true;
    }

    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
