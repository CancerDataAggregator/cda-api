# API Overview

## API Purpose & Key Notes

## Endpoint Summaries

### /data/

### /summary/

### /unique_columns/

### /columns/

### /release_metadata/

## What is a QNode?

#### Overview
A QNode [Query + Node] is the name we have given to the JSON structure used to query the /data/ and /summary/ endpoints. This structure is passed in as the body of the api calls to the aforementioned endpoints. 

```
{
  "MATCH_ALL": [
    "string"
  ],
  "MATCH_SOME": [
    "string"
  ],
  "ADD_COLUMNS": [
    "string"
  ],
  "EXCLUDE_COLUMNS": [
    "string"
  ]
}
```

### MATCH_ALL & MATCH_SOME

**MATCH_ALL** contains a list of column match queries you want to be ***AND***ed together. 

**MATCH_SOME** contains a list of column match queries you want to be ***OR***'d together. 

#### MATCH_ALL Example: &nbsp;&nbsp; *\*(/data/subject endpoint)*
```
{
    MATCH_ALL: [
        "species = human",
        "sex like m*"
    ]
}
```
This example would return data on subjects who are:
- Marked as their species being "human"
<br> ***AND*** 
- Marked as having and observation of their sex start with the letter "m"


#### MATCH_SOME Example: &nbsp;&nbsp; *\*(/data/subject endpoint)*
```
{
    MATCH_SOME: [
        "year_of_birth < 1960", 
        "year_of_birth > 2020"
    ]
}
```
This example would return data on subjects who are:
- Born before 1960 
<br>  ***OR*** 
- Born after 2020

#### MATCH_ALL & MATCH_SOME Example: &nbsp;&nbsp; *\*(/data/subject endpoint)*
```
{
    MATCH_ALL: [
        "species = human",
        "sex like m*"
    ],
    MATCH_SOME: [
        "year_of_birth < 1960", 
        "year_of_birth > 2020"
    ]
}
```
This example would return data on subjects who are:

<br> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(
- Marked as their species being "human"
<br> ***AND*** 
- Marked as having and observation of their sex start with the letter "m"
<br> )
<br> ***AND*** 
<br> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(
    -  Born before 1960 
<br>  ***OR*** 
    - Born after 2020
<br> )


### ADD_COLUMNS and EXCLUDE_COLUMNS

# Endpoint Usage

## /data/

### Parameters & Body Arguments

#### Overview
> - Parameters
>   - **limit** (int, optional): Limit for paged results. Defaults to 100.
>   - **offset** (int, optional): Offset for paged results. Defaults to 0.
> - Body
>   - **qnode** (QNode): JSON input query

#### Explaination

**limit:** This parameter limits the number of rows returned  when hitting this endpoint


### Return

## /summary/

### Parameters & Body Arguments

#### Overview
> - Parameters
>   - **limit** (int, optional): Limit for paged results. Defaults to 100.
>   - **offset** (int, optional): Offset for paged results. Defaults to 0.
> - Body
>   - **qnode** (QNode): JSON input query

#### Examples

### Return

## /unique_values/

### Parameters & Body Arguments

### Return

## /columns/

### Parameters & Body Arguments

### Return

## /release_metadata/

### Parameters & Body Arguments

### Return