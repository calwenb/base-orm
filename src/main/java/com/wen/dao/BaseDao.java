package com.wen.dao;

import com.wen.annotation.FieldName;
import com.wen.annotation.TableName;
import com.wen.wrapper.SetWrapper;
import com.wen.wrapper.WhereWrapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class BaseDao {
    /**
     * <T> 规定泛型化
     */
    public static <T> ArrayList<T> selectTargets(Connection conn, Class<T> targetClass, WhereWrapper... whereWrapper) throws SQLException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException {
        ArrayList<T> targets = new ArrayList<>();

        //反射获取目标信息
        Field[] fields = targetClass.getDeclaredFields();
        TableName tableNameAnno = targetClass.getDeclaredAnnotation(TableName.class);
        String className = targetClass.getSimpleName();

        //确定表名
        String tableName;
        if (tableNameAnno != null) {
            tableName = tableNameAnno.value();
        } else {
            tableName = className;
        }

        //sql拼接
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT * FROM ");
        sql.append(tableName).append(" ");

        List<Object> setFields = null;
        //有Wrapper时
        if (whereWrapper.length != 0) {
            //条件查询
            Map<String, List<Object>> whereRS = whereWrapper[0].getResult();
            Map.Entry<String, List<Object>> head = whereRS.entrySet().iterator().next();
            String whereSQL = head.getKey();
            if (!whereSQL.equals("")) {
                sql.append(" WHERE ");
                sql.append(whereSQL);
            }
            setFields = head.getValue();
        }
        //执行查询
        PreparedStatement pst = conn.prepareStatement(String.valueOf(sql));

        //需要设值时
        for (int i = 0; setFields != null && i < setFields.size(); i++) {
            pst.setObject(i + 1, setFields.get(i));
        }
        System.out.println(pst);
        ResultSet rs = pst.executeQuery();

        //获取全部属性的类
        Class<?>[] classes = new Class[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i].setAccessible(true);
            classes[i] = fields[i].getType();
        }
        //获取类构造器
        Constructor<T> ClassCon = targetClass.getDeclaredConstructor(classes);

        //返回数据解析实体
        while (rs.next()) {
            Object[] fieldsVal = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                fields[i].setAccessible(true);
                //尝试获取属性上注解
                FieldName fieldName = fields[i].getDeclaredAnnotation(FieldName.class);
                if (fieldName != null) {
                    fieldsVal[i] = rs.getObject(String.valueOf(fieldName.value()));
                } else {
                    fieldsVal[i] = rs.getObject(fields[i].getName());
                }

            }
            T target = ClassCon.newInstance(fieldsVal);
            targets.add(target);
        }
        return targets;

    }

    public static <T> T selectTarget(Connection conn, Class<T> targetClass, WhereWrapper... whereWrapper) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        //返回目标
        T target = null;
        //反射获取目标信息
        Field[] fields = targetClass.getDeclaredFields();
        TableName tableNameAnno = targetClass.getDeclaredAnnotation(TableName.class);
        String className = targetClass.getSimpleName();

        //确定表名
        String tableName;
        if (tableNameAnno != null) {
            tableName = tableNameAnno.value();
        } else {
            tableName = className;
        }

        //sql拼接
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT * FROM ").append(tableName);

        List<Object> setFields = null;
        //有Wrapper时
        if (whereWrapper.length != 0) {
            //条件查询
            Map<String, List<Object>> whereRS = whereWrapper[0].getResult();
            Map.Entry<String, List<Object>> head = whereRS.entrySet().iterator().next();
            String whereSQL = head.getKey();
            if (!whereSQL.equals("")) {
                sql.append(" WHERE ");
                sql.append(whereSQL);
            }
            setFields = head.getValue();
        }
        //执行查询
        PreparedStatement pst = conn.prepareStatement(String.valueOf(sql));

        //需要设值时
        for (int i = 0; setFields != null && i < setFields.size(); i++) {
            pst.setObject(i + 1, setFields.get(i));
        }
        System.out.println(pst);
        ResultSet rs = pst.executeQuery();

        //获取全部属性的类
        Class<?>[] classes = new Class[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i].setAccessible(true);
            classes[i] = fields[i].getType();
        }
        //获取类构造器
        Constructor<T> ClassCon = targetClass.getDeclaredConstructor(classes);

        //返回数据解析实体
        while (rs.next()) {
            Object[] fieldsVal = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                fields[i].setAccessible(true);
                //尝试获取属性上注解
                FieldName fieldName = fields[i].getDeclaredAnnotation(FieldName.class);
                if (fieldName != null) {
                    fieldsVal[i] = rs.getObject(String.valueOf(fieldName.value()));
                } else {
                    fieldsVal[i] = rs.getObject(fields[i].getName());
                }

            }
            target = ClassCon.newInstance(fieldsVal);
            break;
        }
        return target;
    }

    public static <T> int insertTarget(Connection conn, T target) throws SQLException, IllegalAccessException {
        Class<?> targetClass = target.getClass();

        //反射获取目标信息
        Field[] fields = targetClass.getDeclaredFields();
        TableName tableNameAnno = targetClass.getDeclaredAnnotation(TableName.class);
        String className = targetClass.getSimpleName();

        //确定表名
        String tableName;
        if (tableNameAnno != null) {
            tableName = tableNameAnno.value();
        } else {
            tableName = className;
        }

        StringBuffer sql = new StringBuffer();
        sql.append("INSERT INTO ").append(tableName);
        sql.append(" ( ");
        for (int i = 1; i < fields.length; i++) {
            String sqlField = "";
            Field field = fields[i];
            field.setAccessible(true);
            if (field.getType().equals(List.class) || field.getType().equals(Map.class)) continue;
            FieldName fieldNameAnno = field.getDeclaredAnnotation(FieldName.class);
            if (fieldNameAnno != null) {
                sqlField = fieldNameAnno.value();
            } else {
                System.out.println(field.getName());
                sqlField = field.getName();
            }
            if (i == fields.length - 1) {
                sql.append(sqlField);
                break;
            }
            sql.append(sqlField).append(",");

        }
        sql.append(" ) ");
        sql.append(" VALUES( ");
        //插入值
        for (int i = 1; i < fields.length; i++) {
            if (i == fields.length - 1) {
                sql.append(" ? ");
                break;
            }
            sql.append(" ?, ");
        }

        sql.append(" ) ");

        PreparedStatement pst = conn.prepareStatement(String.valueOf(sql));
        for (int i = 1; i < fields.length; i++) {
            fields[i].setAccessible(true);
            if (fields[i].getType().equals(List.class) || fields[i].getType().equals(Map.class)) continue;
            pst.setObject(i, fields[i].get(target));
        }
        System.out.println(pst);
        return pst.executeUpdate();
    }

    public static <T> int deleteTarget(Connection conn, Class<T> targetClass, WhereWrapper whereWrapper) throws SQLException {
        //删除必须指定条件，否则会全表删除
        if (whereWrapper == null) {
            System.out.println("删除必须指定条件，否则会全表删除!!!");
            return 0;
        }

        //条件查询，解析where sql
        Map<String, List<Object>> whereRS = whereWrapper.getResult();
        Map.Entry<String, List<Object>> head = whereRS.entrySet().iterator().next();
        String whereSQL = head.getKey();
        if (whereSQL.equals("")) {
            System.out.println("删除必须指定条件，否则会全表删除!!!");
            return 0;
        }

        List<Object> setFields = head.getValue();

        //反射获取目标信息
        TableName tableNameAnno = targetClass.getDeclaredAnnotation(TableName.class);
        String className = targetClass.getSimpleName();

        //确定表名
        String tableName;
        if (tableNameAnno != null) {
            tableName = tableNameAnno.value();
        } else {
            tableName = className;
        }

        StringBuffer sql = new StringBuffer();
        sql.append("DELETE FROM ").append(tableName);
        sql.append(" WHERE ").append(whereSQL);

        //执行查询
        PreparedStatement pst = conn.prepareStatement(String.valueOf(sql));

        // ? 设值
        for (int i = 0; setFields != null && i < setFields.size(); i++) {
            pst.setObject(i + 1, setFields.get(i));
        }
        System.out.println(pst);
        return pst.executeUpdate();
    }

    public static <T> int updateTarget(Connection conn, Class<T> targetClass, SetWrapper setWrapper, WhereWrapper whereWrapper) throws SQLException {
        //更新必须指定条件
        if (setWrapper == null || whereWrapper == null) {
            System.out.println("更新必须指定set,where");
            return 0;
        }
        //条件查询，解析where sql
        Map<String, List<Object>> whereRS = whereWrapper.getResult();
        Map.Entry<String, List<Object>> whereHead = whereRS.entrySet().iterator().next();
        String whereSQL = whereHead.getKey();

        //条件查询，解析set sql
        Map<String, List<Object>> setRS = setWrapper.getResult();
        Map.Entry<String, List<Object>> setHead = setRS.entrySet().iterator().next();
        String setSQL = setHead.getKey();
        if (setSQL.equals("") || whereSQL.equals("")) {
            System.out.println("更新必须指定set,where");
            return 0;
        }

        //反射获取目标信息
        TableName tableNameAnno = targetClass.getDeclaredAnnotation(TableName.class);
        String className = targetClass.getSimpleName();

        //确定表名
        String tableName;
        if (tableNameAnno != null) {
            tableName = tableNameAnno.value();
        } else {
            tableName = className;
        }

        //拼接sql
        StringBuffer sql = new StringBuffer();
        sql.append("UPDATE ").append(tableName);
        sql.append(" SET ").append(setSQL);
        sql.append(" WHERE ").append(whereSQL);

        //执行查询
        PreparedStatement pst = conn.prepareStatement(String.valueOf(sql));

        // ?设值
        List<Object> setFields = new ArrayList<>();
        setFields.addAll(setHead.getValue());
        setFields.addAll(whereHead.getValue());
        for (int i = 0; i < setFields.size(); i++) {
            pst.setObject(i + 1, setFields.get(i));
        }
        System.out.println(pst);
        return pst.executeUpdate();
    }

}
