CREATE TABLE accounts (
    account_id varchar(255) NOT NULL,
    name varchar(255),
    parent_account_id varchar(255),
    status varchar(32),
    last_modified timestamp without time zone,
    agent_id varchar(255) NOT NULL,
    _original jsonb,
    PRIMARY KEY(account_id, agent_id)
);

CREATE TABLE users (
    user_id varchar(255) NOT NULL,
    email varchar(255),
    login_id varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    status varchar(255),
	last_modified timestamp,
	agent_id varchar(255) NOT NULL,
    _original jsonb,
	PRIMARY KEY(user_id, agent_id)
);

CREATE TABLE courses (
    course_id varchar(255) NOT NULL,
    account_id varchar(255) NOT NULL,
    short_name varchar(255),
    long_name varchar(255),
    status varchar(255),
	last_modified timestamp,
	agent_id varchar(255) NOT NULL,
    _original jsonb,
	PRIMARY KEY(course_id, agent_id)
);

CREATE TABLE sections (
    section_id varchar(255) NOT NULL,
    course_id varchar(255) NOT NULL,
    name varchar(255),
    status varchar(255),
	last_modified timestamp,
	agent_id varchar(255) NOT NULL,
    _original jsonb,
	PRIMARY KEY(section_id, agent_id)
);

CREATE TABLE terms (
    term_id varchar(255) NOT NULL,
    name varchar(255),
    start_date varchar(255),
    end_date varchar(255),
    status varchar(255),
	last_modified timestamp,
	agent_id varchar(255) NOT NULL,
    _original jsonb,
	PRIMARY KEY(term_id, agent_id)
);

CREATE TABLE enrollments (
    user_id varchar(255) NOT NULL,
    section_id varchar(255) NOT NULL,
    role varchar(255),
    status varchar(255),
	last_modified timestamp,
	agent_id varchar(255) NOT NULL,
    _original jsonb,
	PRIMARY KEY(user_id, section_id, agent_id)
);

ALTER TABLE public.accounts OWNER TO postgres;

