import polars as pl
from pydantic import BaseModel, ConfigDict

from mo.metrics.domain.config import Config
from mo.metrics.usecases.usecase import UseCase


class Input(Config):
    data: pl.DataFrame | pl.LazyFrame


class Output(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: pl.LazyFrame


class ExtractDemographicsUseCase(UseCase):
    Input = Input

    def execute(self, input: Input) -> Output:
        return Output(data=extract_demographics(input.data))


# Important note: the following code is only correct if students do not self-identify as
# two different races. As of 2023-08-10, the data did not contain any K12 students
# who fit this description (checked by getting unique responses and visually verifying
# that all values were single element lists), so the code is correct for those students.
def extract_demographics(responses_df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """Extract demographic info from a responses data frame."""
    prompts = [
        *prompt_maps["gender"],
        *prompt_maps["gender_other"],
        *prompt_maps["race"],
        *prompt_maps["race_other"],
        *prompt_maps["maternal_education"],
    ]

    prompt = pl.col("prompt")
    response = pl.col("response")
    demographic_type = pl.col("demographic_type")
    result = (
        responses_df.lazy()
        .filter(
            pl.col("dt_submitted").is_not_null(),
            pl.col("prompt").is_in(prompts),
        )
        .with_columns(
            demographic_type=pl.when(prompt.is_in(prompt_maps["gender"]))
            .then(pl.lit("gender"))
            .when(prompt.is_in(prompt_maps["race"]))
            .then(pl.lit("race"))
            .when(prompt.is_in(prompt_maps["maternal_education"]))
            .then(pl.lit("maternal_education"))
            .cast(pl.Categorical)
        )
        .filter(demographic_type.is_not_null())
        .with_columns(
            response=pl.when(demographic_type == "gender")
            .then(response.replace(response_maps["gender"], default=None))
            .when(demographic_type == "race")
            .then(response.replace(response_maps["race"], default=None))
            .when(demographic_type == "maternal_education")
            .then(response)
        )
        .filter(response.is_not_null())
        .sort("dt_submitted")
        .unique(["student_id", "demographic_type", "response"], keep="first")
        .select(pl.col("student_id", "demographic_type", "response"))
    )

    return (
        result.collect()
        .pivot(
            "demographic_type",
            index="student_id",
            values="response",
            aggregate_function="first",
            sort_columns=True,
        )
        .with_columns(
            pl.col("gender").cast(pl.Categorical("lexical")),
            pl.col("race").cast(pl.Categorical("lexical")),
        )
        .lazy()
    )


prompt_maps = {
    "gender": [
        "What best describes your gender?",
        "<p><strong>Now just a few demographic questions. Your answers to these questions, as with all your answers on this survey, are kept confidential. Although we hope that you'll respond, you may skip questions you do not wish to answer.</strong></p>\n\n<p>What best describes your gender?</p>\n",
    ],
    "gender_other": [
        "If you indicated that you prefer to self-describe your gender, how do you describe yourself?",
    ],
    "maternal_education": [
        "To the best of your knowledge, what is the HIGHEST level of education earned by your mother?",
    ],
    "race": [
        "Which of the following categories describe you? Select all that apply.",
        'Which of the following categories describe you? If you\'d like to use more than one category or if you\'d like to use a category not provided, please select ""prefer to self-describe"" and answer the next question.',
        '<span style=""font-size:0.8em"">What is your racial/ethnic background?</span>',
        "What best describes your racial/ethnic background?",
        "What is your racial/ethnic background?",
        "Which of the following categories describe you? If you'd like to use more than one category or if you'd like to use a category not provided, please select \"prefer to self-describe\" and answer the next question."
        '<span style="font-size:0.8em">If you selected "Other" racial/ethnic background, how do you describe yourself?</span>',
        '<span style="font-size:0.8em">What is your racial/ethnic background?</span>',
        "Which of the following categories describe you? If you'd like to use more than one category or if you'd like to use a category not provided, please select \"prefer to self-describe\" and answer the next question.",
    ],
    "race_other": [
        "If you prefer to self-describe your race, identity, or national origin, how do you describe yourself?",
        '<span style=""font-size:0.8em"">If you selected ""Other"" racial/ethnic background, how do you describe yourself?</span>',
        'If you selected ""Other"" racial/ethnic background, how do you describe yourself?',
        "If you indicated in the prior question that you prefer to self-describe your racial/ethnic background, how do you describe yourself?"
        "If you prefer to self-describe your race, identity, or national origin, how do you describe yourself?",
        "If you prefer to self-describe your race, identity, or national origin, how do you describe yourself?"
        "If you indicated in the prior question that you prefer to self-describe your racial/ethnic background, how do you describe yourself?",
        'If you selected "Other" racial/ethnic background, how do you describe yourself?',
        '<span style="font-size:0.8em">If you selected "Other" racial/ethnic background, how do you describe yourself?</span>',
        '<span style="font-size:0.8em">If you selected "Other" racial/ethnic background, how do you describe yourself?</span>',
    ],
}

response_maps = {
    "gender": {
        "Female": "Female",
        "Male": "Male",
        "Non-binary": "Non-binary",
        "Other": "Prefer to self-describe",
    },
    "race": {
        "American Indian or Alaska Native": "American Indian or Alaska Native",
        "<strong>American Indian or Alaska Native</strong> (Examples include Navajo Nation, Blackfeet Tribe, Mayan, Aztec, Nome Eskimo Community, etc)": "American Indian or Alaska Native",
        "Asian or Asian Am.": "Asian or Asian Am.",
        "Asian": "Asian or Asian Am.",
        "<strong>Asian or Asian Am.</strong> (Examples include Asian American, Chinese, Fillipino, Asian Indian, Vietnamese, Korean, etc.)": "Asian or Asian Am.",
        "Black or African Am.": "Black or African Am.",
        "Black": "Black or African Am.",
        "African-American": "Black or African Am.",
        "<strong>Black or African Am.</strong> (Examples include African American, Jamaican, Haitian, Nigerian, Ethiopian, Somalian, etc.)": "Black or African Am.",
        "Hispanic, Latino, or Spanish Origin": "Hispanic, Latino, or Spanish Origin",
        "Latin": "Hispanic, Latino, or Spanish Origin",
        "Latinx": "Hispanic, Latino, or Spanish Origin",
        "<strong>Hispanic, Latino, or Spanish Origin</strong> (Examples include Mexican or Mexican American, Puerto Rican, Cuban, Salvadorian, Dominican, Columbian, etc.)": "Hispanic, Latino, or Spanish Origin",
        "Middle Eastern or North African": "Middle Eastern or North African",
        "<strong>Middle Eastern or North African</strong> (Examples include Lebanese, Iranian, Egyptian, Syrian, Moroccan, Algerian, etc.)": "Middle Eastern or North African",
        "Native Hawaiian or Pacific Islander": "Native Hawaiian or Pacific Islander",
        "<strong>Native Hawaiian or Pacific Islander</strong> (Examples include Native Hawaiian, Samoan, Chamorro, Tongan, Fijian, Marshalese, etc)": "Native Hawaiian or Pacific Islander",
        "White": "White",
        "<strong>White</strong> (Examples include German, Irish, English, French, Norwegian, etc.)": "White",
        "Other": "Other",
        "Prefer to self-describe": "Other",
        "Prefer to self-describe my race, ethnicity, or national origin": "Other",
        "Prefer not to say": "Prefer not to say",
    },
}
