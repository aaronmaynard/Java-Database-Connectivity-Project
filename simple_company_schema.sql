-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema simple_company
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema simple_company
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `simple_company` DEFAULT CHARACTER SET utf8 ;
USE `simple_company` ;

-- -----------------------------------------------------
-- Table `simple_company`.`CUSTOMER`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `simple_company`.`CUSTOMER` (
  `id` INT NOT NULL,
  `first_name` VARCHAR(45) NOT NULL,
  `last_name` VARCHAR(45) NOT NULL,
  `gender` CHAR(1) NOT NULL,
  `dob` DATE NOT NULL,
  `email` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `email_UNIQUE` (`email` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `simple_company`.`PRODUCT`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `simple_company`.`PRODUCT` (
  `id` INT NOT NULL,
  `prod_name` VARCHAR(45) NULL,
  `prod_desc` VARCHAR(1024) NULL,
  `prod_category` INT NULL,
  `prod_upc` CHAR(12) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `simple_company`.`PURCHASE`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `simple_company`.`PURCHASE` (
  `id` INT NOT NULL,
  `purchase_date` TIMESTAMP NULL,
  `purchase_amt` DECIMAL(9,2) NULL,
  `PRODUCT_id` INT NULL,
  `CUSTOMER_id` INT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_PURCHASE_CUSTOMER_idx` (`CUSTOMER_id` ASC),
  INDEX `fk_PURCHASE_PRODUCT1_idx` (`PRODUCT_id` ASC),
  CONSTRAINT `fk_PURCHASE_CUSTOMER`
    FOREIGN KEY (`CUSTOMER_id`)
    REFERENCES `simple_company`.`CUSTOMER` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_PURCHASE_PRODUCT1`
    FOREIGN KEY (`PRODUCT_id`)
    REFERENCES `simple_company`.`PRODUCT` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `simple_company`.`ADDRESS`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `simple_company`.`ADDRESS` (
  `address1` VARCHAR(45) NOT NULL,
  `address2` VARCHAR(45) NULL,
  `city` VARCHAR(45) NOT NULL,
  `state` VARCHAR(45) NOT NULL,
  `zipcode` VARCHAR(45) NOT NULL,
  `CUSTOMER_id` INT NOT NULL,
  INDEX `fk_ADDRESS_CUSTOMER1_idx` (`CUSTOMER_id` ASC),
  CONSTRAINT `fk_ADDRESS_CUSTOMER1`
    FOREIGN KEY (`CUSTOMER_id`)
    REFERENCES `simple_company`.`CUSTOMER` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `simple_company`.`CREDIT_CARD`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `simple_company`.`CREDIT_CARD` (
  `name` VARCHAR(45) NOT NULL,
  `cc_number` VARCHAR(45) NULL,
  `exp_date` VARCHAR(45) NULL,
  `security_code` VARCHAR(45) NULL,
  `CUSTOMER_id` INT NULL,
  INDEX `fk_CREDIT_CARD_CUSTOMER1_idx` (`CUSTOMER_id` ASC),
  CONSTRAINT `fk_CREDIT_CARD_CUSTOMER1`
    FOREIGN KEY (`CUSTOMER_id`)
    REFERENCES `simple_company`.`CUSTOMER` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
