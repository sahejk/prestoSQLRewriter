import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.google.common.collect.ImmutableList;
import jdk.nashorn.internal.ir.annotations.Immutable;

import java.util.*;

public class SQLRewrite {

    public static void main(String args[]) {
        }

    public static Query rewriteQuery(Query query) {
        Optional<With> with = query.getWith();
        QueryBody queryBody = query.getQueryBody();
        if (queryBody instanceof SetOperation) {
            queryBody = handleSetOperation((SetOperation) queryBody);
        }
        if (queryBody instanceof QuerySpecification) {
            queryBody = handleQuerySpecification((QuerySpecification) queryBody);
        }
        Optional<With> with1 = Optional.empty();
        if (with.isPresent()) {
            Iterator<WithQuery> iterator = with.get().getQueries().iterator();
            List<WithQuery> withQueries = new ArrayList<>();
            while (iterator.hasNext()) {
                WithQuery withQuery = iterator.next();
                withQueries.add(new WithQuery(withQuery.getName(), rewriteQuery(withQuery.getQuery()), withQuery.getColumnNames()));
            }
            with1 = Optional.of(new With(with.isPresent(), withQueries));
        }
        return new Query(with1, queryBody, query.getOrderBy(), query.getLimit());
    }

    public static SetOperation handleSetOperation(SetOperation queryBody) {
        Iterator<Relation> iterator = queryBody.getRelations().iterator();
        int index = 0;
        List setoperations = new ArrayList<Relation>();
        while (iterator.hasNext()) {
            Relation next = iterator.next();
            if (next instanceof QuerySpecification) {
                setoperations.add(handleQuerySpecification((QuerySpecification) next));
            } else if (next instanceof SetOperation) {
                setoperations.add(handleSetOperation((SetOperation) next));
            } else {
                setoperations.add(next);
            }
        }
        if(queryBody instanceof Union) {
            return new Union(setoperations,queryBody.isDistinct());
        }
        return new Intersect(setoperations,queryBody.isDistinct());
    }

    public static QuerySpecification handleQuerySpecification(QuerySpecification queryBody) {
        Optional<Expression> where = queryBody.getWhere();
        Optional<Relation> from = queryBody.getFrom();
        if (from.isPresent()) {
            Relation relation = from.get();
            if (relation instanceof Table) {
                List<String> parts = ((Table) relation).getName().getParts();
                if (parts.contains("fpa_dt_actuals") || parts.contains("fpa_dt_fcst") || parts.contains("fpa_dt_capex")) {
                    Expression inListExpression = new InListExpression(ImmutableList.of(new StringLiteral("4140")));
                    Expression inPredicate = new InPredicate(new StringLiteral("cost_center"), inListExpression);
                    Expression finalExpression = where.isPresent() ? new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, where.get(), inPredicate)
                            : inPredicate;
                    return new QuerySpecification(queryBody.getSelect(), queryBody.getFrom(), Optional.of(finalExpression), queryBody.getGroupBy(), queryBody.getHaving(), queryBody.getOrderBy(), queryBody.getLimit());
                }
            }
            if (relation instanceof Join) {
                Join relationNew = (Join) relation;
                HashMap<String, String> tablesFromJoin = (HashMap<String, String>) getTablesFromJoin(relationNew);
                Object[] keys = tablesFromJoin.keySet().toArray();
                Optional<Expression> finalExpression = where.isPresent() ? Optional.of(where.get()) : Optional.empty();
                for (Object key: keys) {
                    if (key.equals("fpa_dt_actuals") || key.equals("fpa_dt_fcst") || key.equals("fpa_dt_capex")) {
                        Expression inListExpression = new InListExpression(ImmutableList.of(new StringLiteral("4140")));
                        Expression inPredicate = new InPredicate(new StringLiteral(tablesFromJoin.get(key) + "."+ "cost_center"), inListExpression);
                        finalExpression = finalExpression.isPresent() ? Optional.of(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, finalExpression.get(), inPredicate))
                                : Optional.of(inPredicate);
                    }
                }
                return new QuerySpecification(queryBody.getSelect(), Optional.of(handleJoin(relationNew)), finalExpression, queryBody.getGroupBy(), queryBody.getHaving(), queryBody.getOrderBy(), queryBody.getLimit());
            }
            if(relation instanceof  AliasedRelation) {
                AliasedRelation relation1 = (AliasedRelation) relation;
                String alias =  relation1.getAlias().getValue();
                String name = ((Table)relation1.getRelation()).getName().getParts().get(0);
                if (name.equals("fpa_dt_actuals") || name.equals("fpa_dt_fcst") || name.equals("fpa_dt_capex")) {
                    Expression inListExpression = new InListExpression(ImmutableList.of(new StringLiteral("4140")));
                    Expression inPredicate = new InPredicate(new StringLiteral(alias + "."+ "cost_center"), inListExpression);
                    Expression finalExpression = where.isPresent() ? new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, where.get(), inPredicate)
                            : inPredicate;
                    return new QuerySpecification(queryBody.getSelect(), queryBody.getFrom(), Optional.of(finalExpression), queryBody.getGroupBy(), queryBody.getHaving(), queryBody.getOrderBy(), queryBody.getLimit());

                }
            }
        }
        return queryBody;
    }

    public static Map<String, String> getTablesFromJoin(Join join) {
        Map<String, String> tables = new HashMap<>();
        if(join.getLeft() instanceof Table) {
            Table left = (Table) join.getLeft();
            String s = left.getName().getParts().get(0);
            tables.put(s,s);
        }
        if(join.getLeft() instanceof AliasedRelation) {
            AliasedRelation left = (AliasedRelation) join.getLeft();
            if(left.getRelation() instanceof Table){
            String alias = left.getAlias().getValue();
            String s = ((Table)(left.getRelation())).getName().getParts().get(0);
            tables.put(s,alias);
            }
        }
        if(join.getRight() instanceof Table) {
            Table left = (Table) join.getRight();
            String s = left.getName().getParts().get(0);
            tables.put(s,s);
        }
        if(join.getRight() instanceof AliasedRelation) {
            AliasedRelation right = (AliasedRelation) join.getRight();
            if(right.getRelation() instanceof  Table) {
                String alias = right.getAlias().getValue();
                String s = ((Table) (right.getRelation())).getName().getParts().get(0);
                tables.put(s, alias);
            }
        }
        if(join.getLeft() instanceof Join) {
            tables.putAll(getTablesFromJoin((Join) join.getLeft()));
        }
        if(join.getRight() instanceof Join) {
            tables.putAll(getTablesFromJoin((Join) join.getRight()));
        }
        return tables;
    }

    public static Join handleJoin(Join join) {
        return new Join(join.getLocation().get(),
                join.getType(),
                handleRelation(join.getLeft()),
                handleRelation(join.getRight()),
                join.getCriteria());
    }

    public static Relation handleRelation(Relation relation) {
        if (relation instanceof Join) {
            return handleJoin((Join) relation);
        }
        if (relation instanceof AliasedRelation) {
            AliasedRelation relation1 = (AliasedRelation) relation;
            return new AliasedRelation(relation1.getLocation().get(),handleRelation(relation1.getRelation()),relation1.getAlias(),relation1.getColumnNames());
        }
        if(relation instanceof TableSubquery) {
            return new TableSubquery(relation.getLocation().get(),rewriteQuery(((TableSubquery) relation).getQuery()));
        }
        return relation;
    }
}
