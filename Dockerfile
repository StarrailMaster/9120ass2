# 使用 Maven 官方镜像
FROM maven:3.9.9-eclipse-temurin-17 AS build

# 设置工作目录
WORKDIR /app

# 复制 pom.xml 到容器中
COPY pom.xml .

# 将 Maven 依赖项预先构建以利用缓存
RUN mvn dependency:go-offline

# 复制项目代码到容器中
COPY src/ /app/src/
COPY src/main/resources /app/src/main/resources
# 构建项目
RUN mvn clean package

# 使用 OpenJDK 17 作为基础镜像来运行应用程序
FROM eclipse-temurin:17-jdk

# 将构建的 jar 文件复制到新的镜像中
COPY --from=build /app/target/Intelligent-Ecommerce-Application-0.0.1-SNAPSHOT.jar /app.jar

# 暴露端口（根据您的应用程序设置）
EXPOSE 8080

# 启动应用程序
CMD ["java", "-jar", "/app.jar"]

