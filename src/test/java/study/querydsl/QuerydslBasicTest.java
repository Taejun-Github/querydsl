package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;
import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @PersistenceContext
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");

        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member2", 30, teamB);
        Member member4 = new Member("member2", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

    }

    @Test
    public void startJPQL() {
        // member1을 찾는다.
        String qlString = "select m from Member m where " +
                "m.username = :username";
        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();
        Assertions.assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {

        queryFactory = new JPAQueryFactory(em);

        QMember m = member;
        // QMember 내부에 들어가는 것은 별칭이므로 중요하지 않음

        Member findMember = queryFactory
                .select(m)
                .from(m)
                .where(m.username.eq("member1")).fetchOne();

        Assertions.assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {

        queryFactory = new JPAQueryFactory(em);

        QMember m = member;

        Member findMember = queryFactory
                .select(m)
                .from(m)
                .where(m.username.eq("member1").and(m.age.between(10, 30))).fetchOne();


        Assertions.assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        queryFactory = new JPAQueryFactory(em);

        QMember m = member;

        List<Member> fetch = queryFactory.selectFrom(m).fetch();

        Member fetchOne = queryFactory.selectFrom(m).fetchOne();

        Member fetchFirst = queryFactory.selectFrom(m).fetchFirst();

        QueryResults<Member> results = queryFactory.selectFrom(m).fetchResults();
        long total = results.getTotal();
        List<Member> content = results.getResults();
        long limit = results.getLimit();
    }

    @Test
    public void sort() {
        queryFactory = new JPAQueryFactory(em);

        QMember m = member;
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory.selectFrom(m).where(m.age.eq(100)).orderBy(m.age.desc(), m.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        Assertions.assertThat(member5.getUsername()).isEqualTo("member5");
        Assertions.assertThat(member6.getUsername()).isEqualTo("member6");
        Assertions.assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging1() {
        queryFactory = new JPAQueryFactory(em);

        QMember m = member;

        List<Member> result = queryFactory.selectFrom(m).orderBy(m.username.desc()).offset(1).limit(2).fetch();
        Assertions.assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        queryFactory = new JPAQueryFactory(em);

        QMember m = member;

        QueryResults<Member> results = queryFactory.selectFrom(m).orderBy(m.username.desc()).offset(1).limit(2).fetchResults();
        Assertions.assertThat(results.getTotal()).isEqualTo(4);
        Assertions.assertThat(results.getLimit()).isEqualTo(2);
        Assertions.assertThat(results.getOffset()).isEqualTo(1);
    }

    @Test
    public void aggregation() {
        queryFactory = new JPAQueryFactory(em);

        QMember m = member;

        List<Tuple> result = queryFactory
                .select(
                        m.count(),
                        m.age.sum(),
                        m.age.avg(),
                        m.age.max(),
                        m.age.min()
                )
                .from(m)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple.get(m.count()));
        }
    }

    @Test
    public void group() throws Exception {
        queryFactory = new JPAQueryFactory(em);

        QMember m = member;

        List<Tuple> result = queryFactory.select(team.name, m.age.avg())
                .from(m)
                .join(m.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        Assertions.assertThat(teamA.get(team.name)).isEqualTo("teamA");
        Assertions.assertThat(teamA.get(m.age.avg())).isEqualTo(15);
    }

    @Test
    public void join() {
        queryFactory = new JPAQueryFactory(em);

        QMember m = member;

        List<Member> result = queryFactory.selectFrom(m).join(m.team, team)
                .where(team.name.eq("teamA"))
                .fetch();
        Assertions.assertThat(result).extracting("username").containsExactly("member1", "member2");
    }

    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        queryFactory = new JPAQueryFactory(em);


        queryFactory.select(member).from(member, team).where(member.username.eq(team.name))
                .fetch();
    }

    @Test
    public void join_on_filtering() {
        queryFactory = new JPAQueryFactory(em);

        List<Tuple> team = queryFactory.select(QMember.member, QMember.member.team)
                .from(QMember.member).join(QMember.member.team, QTeam.team)
                .on(QTeam.team.name.eq("teamA")).fetch();

        for (Tuple tuple : team) {
            System.out.println(tuple);
        }
    }

    @Test
    public void join_on_no_relation() {
        queryFactory = new JPAQueryFactory(em);
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team)
                .on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        queryFactory = new JPAQueryFactory(em);
        Member findMember = queryFactory.selectFrom(member).where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());

    }

    @Test
    public void fetchJoinUse() {
        em.flush();
        em.clear();

        queryFactory = new JPAQueryFactory(em);
        Member findMember = queryFactory.selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());

    }

    @Test
    public void subQuery() {

        QMember memberSub = new QMember("memberSub");
        // 멤버가 겹치면 안되므로 새로운 멤버를 만들어서 써줘야 한다.

        queryFactory = new JPAQueryFactory(em);
        List<Member> result = queryFactory.selectFrom(member)
                .where(member.age.eq(select(memberSub.age.max())
                        .from(memberSub)
                ))
                .fetch();

        Assertions.assertThat(result).extracting("age").containsExactly(40);
    }

    @Test
    public void subQueryGoe() {

        QMember memberSub = new QMember("memberSub");
        // 멤버가 겹치면 안되므로 새로운 멤버를 만들어서 써줘야 한다.

        queryFactory = new JPAQueryFactory(em);
        List<Member> result = queryFactory.selectFrom(member)
                .where(member.age.goe(select(memberSub.age.avg())
                        .from(memberSub)
                ))
                .fetch();

        Assertions.assertThat(result).extracting("age").containsExactly(30, 40);
    }

    @Test
    public void subQueryIn() {

        QMember memberSub = new QMember("memberSub");
        // 멤버가 겹치면 안되므로 새로운 멤버를 만들어서 써줘야 한다.

        queryFactory = new JPAQueryFactory(em);
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(select(memberSub.age).
                        from(memberSub).
                        where(memberSub.age.gt(10))
                )).fetch();

        Assertions.assertThat(result).extracting("age").containsExactly(30, 40);
    }

    @Test
    public void selectSubquery() {

        QMember memberSub = new QMember("memberSub");
        queryFactory = new JPAQueryFactory(em);
        List<Tuple> result = queryFactory.select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub))
                .from(member).fetch();


        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
    }

    @Test
    public void basicCase() {
        queryFactory = new JPAQueryFactory(em);
        queryFactory.select(member.age
                .when(10).then("열살")
                .when(20).then("20살")
                .otherwise("기타"))
                .from(member)
                .fetch();
    }

    @Test
    public void complexCase() {
        queryFactory = new JPAQueryFactory(em);
        queryFactory.select(new CaseBuilder()
                .when(member.age.between(0, 20)).then("0~20살")
                .when(member.age.between(21, 30)).then("21~30")
                .otherwise("기타"))
                .from(member)
                .fetch();
    }

    @Test
    public void constant() {
        queryFactory = new JPAQueryFactory(em);
        List<Tuple> result = queryFactory.select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
    }

    @Test
    public void concat() {
        queryFactory = new JPAQueryFactory(em);
        List<String> fetch = queryFactory.select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .fetch();

        for (String s : fetch) {
            System.out.println(s);
        }
    }

    @Test
    public void simpleProjection() {
        queryFactory = new JPAQueryFactory(em);
        List<String> result = queryFactory.select(member.username).from(member).fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }

    @Test
    public void tupleProjection() {
        queryFactory = new JPAQueryFactory(em);
        List<Tuple> result = queryFactory.select(member.username, member.age)
                .from(member).fetch();

        // 이렇게 반환타입을 어러개로 하면 튜플 타입이 반환된다.

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println(username + age);
        }
    }

    @Test
    public void findDtoByJPQL() {
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void findDtoBySetter() {
        queryFactory = new JPAQueryFactory(em);
        List<MemberDto> result = queryFactory.select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void findDtoByField() {
        queryFactory = new JPAQueryFactory(em);
        List<MemberDto> result = queryFactory.select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void findDtoByConstructor() {
        queryFactory = new JPAQueryFactory(em);
        List<MemberDto> result = queryFactory.select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }
    
    @Test
    public void findUserDto() {
        queryFactory = new JPAQueryFactory(em);
        QMember memberSub = new QMember("memberSub");
        List<UserDto> result = queryFactory.select(Projections.fields(UserDto.class, member.username.as("name"),
                ExpressionUtils.as(JPAExpressions.select(memberSub.age.max())
                        .from(memberSub), "age")
        )).from(member).fetch();
    }

    @Test
    public void findDtoByQueryProjection() {
        queryFactory = new JPAQueryFactory(em);
        List<MemberDto> result = queryFactory.select(new QMemberDto(member.username, member.age)).from(member).fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }


    }

    @Test
    public void dynamicQuery_BooleanBuilder() {
        queryFactory = new JPAQueryFactory(em);
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember1(usernameParam, ageParam);

        for (Member member : result) {
            System.out.println(member);
        }

    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder();

        if(usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
            // null 이 아닐 시에 조건을 and 조건으로 넣어준다.
        }

        if(ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory.selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    public void dynamicQuery_WhereParam() {
        queryFactory = new JPAQueryFactory(em);
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember2(usernameParam, ageParam);
        Assertions.assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return queryFactory.selectFrom(member)
                .where(allEq(usernameCond, ageCond))
                .fetch();
    }

    private BooleanExpression usernameEq(String usernameCond) {
        if(usernameCond == null) {
            return null; // null이 반환되면 위의 where에서 자동으로 무시한다.
        }
        return member.username.contains(usernameCond);
    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.gt(ageCond) : null;
    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    @Test
    @Transactional
    public void bulkUpdate() {
        queryFactory = new JPAQueryFactory(em);
        queryFactory.update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        em.flush();
        em.clear();

        List<Member> result = queryFactory.selectFrom(member).fetch();
    }

    @Test
    @Commit
    public void bulkAdd() {
        queryFactory = new JPAQueryFactory(em);
        long count = queryFactory.update(member)
                .set(member.age, member.age.add(1))
                .execute();
    }

    @Test
    @Commit
    public void bulkDelete() {
        queryFactory = new JPAQueryFactory(em);
        long execute = queryFactory.delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    @Test
    public void sqlFunction() {
        queryFactory = new JPAQueryFactory(em);
        List<String> result = queryFactory.select(
                        Expressions.stringTemplate("function('replace', {0}, {1}, {2}",
                                member.username, "member", "M"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }

    @Test
    @Transactional
    public void sqlFunction2() {
        queryFactory = new JPAQueryFactory(em);
        List<String> result = queryFactory.select(member.username)
                .from(member)
                .where(member.username.eq(
                        Expressions.stringTemplate("function('lower', {0})", member.username)))
                .fetch();
    }
}
