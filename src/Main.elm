port module Main exposing (..)

import Tuple exposing (..)
import String exposing (..)
import StringUtils exposing (..)
import Dict exposing (Dict)
import Maybe.Extra as Maybe exposing (isNothing)
import Slate.TestEntities.Person.Entity as Person exposing (..)
import Slate.TestEntities.Address.Entity as Address exposing (..)
import Slate.TestEntities.Person.Schema as Person exposing (..)
import Slate.TestEntities.Address.Schema as Address exposing (..)
import Slate.Engine.Query as Query exposing (..)
import Slate.Engine.Engine as Engine exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Projection exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.Mutation as Mutation exposing (..)
import Slate.Common.Db exposing (..)
import Utils.Ops exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import ParentChildUpdate exposing (..)


port node : Float -> Cmd msg


engineDBInfo : DbConnectionInfo
engineDBInfo =
    { host = "testEntitiesServer"
    , port_ = 5432
    , database = "test_entities"
    , user = "postgres"
    , password = "password"
    , timeout = 15000
    }


engineConfig : Engine.Config Msg
engineConfig =
    { connectionRetryMax = 3
    , debug = True
    , logTagger = EngineLog
    , errorTagger = EngineError
    , eventProcessingErrorTagger = EventProcessingError
    , completionTagger = EventProcessingComplete
    , routeToMeTagger = EngineModule
    , queryBatchSize = 2 -- kept small to guarantee that multiple event records batches will be retrieved
    }


{-|

    Avoid infinitely recursive definition in Model.
-}
type WrappedModel
    = WrappedModel Model


type alias PersonDict =
    EntityDict Person


type alias AddressDict =
    EntityDict Address


type alias Model =
    { personFragments : Person.FragmentDict
    , addressFragments : Address.FragmentDict
    , persons : PersonDict
    , addresses : AddressDict
    , engineModel : Engine.Model Msg
    , queries : Dict Int (WrappedModel -> Result ProjectionErrors WrappedModel)
    , didRefresh : Bool
    }


type alias Person =
    { name : Person.Name
    , address : Address
    , aliases : List String
    , oldAddresses : List String
    }


type alias Address =
    { street : String
    }


type alias Entities =
    { persons : Person
    }


executeQuery : Engine.Model Msg -> Maybe String -> Query Msg -> List String -> Result (List String) ( Engine.Model Msg, Cmd Msg, Int )
executeQuery =
    Engine.executeQuery engineConfig engineDBInfo


refreshQuery : Engine.Model Msg -> Int -> Result String ( Engine.Model Msg, Cmd Msg )
refreshQuery =
    Engine.refreshQuery engineConfig engineDBInfo


initModel : ( Model, List (Cmd Msg) )
initModel =
    let
        ( engineModel, engineCmd ) =
            Engine.init engineConfig
    in
        ( { personFragments = Dict.empty
          , addressFragments = Dict.empty
          , persons = Dict.empty
          , addresses = Dict.empty
          , engineModel = engineModel
          , queries = Dict.empty
          , didRefresh = False
          }
        , [ engineCmd ]
        )


init : ( Model, Cmd Msg )
init =
    let
        ( model, cmds ) =
            initModel

        result =
            executeQuery model.engineModel Nothing personQuery [ "123", "456" ]

        -- executeQuery initModel.engineModel (Just "id NOT IN (3, 7)") personQuery [ "123", "456" ]
    in
        result
            |??>
                (\( engineModel, cmd, queryId ) ->
                    { model | engineModel = engineModel, queries = Dict.insert queryId projectPerson model.queries } ! [ cmd ]
                )
            ??= (\errs -> Debug.crash <| "Init error:" +-+ (String.join "\n" errs))


main : Program Never Model Msg
main =
    Platform.program
        { init = init
        , update = update
        , subscriptions = subscriptions
        }


mutationError : String -> Model -> String -> ( ( Model, Cmd Msg ), List msg )
mutationError entityName model error =
    let
        l =
            Debug.crash <| "Cannot mutate model for:" +-+ entityName +-+ "Error:" +-+ error
    in
        ( model ! [], [] )


deleteTaggers : CascadingDeletionTaggers Msg
deleteTaggers =
    Dict.fromList [ ( "Address", MutateAddress ) ]


type Msg
    = Nop
    | EngineModule Engine.Msg
    | EventError EventRecord ( QueryId, String )
    | EngineLog ( LogLevel, ( QueryId, String ) )
    | EngineError ( ErrorType, ( QueryId, String ) )
    | EventProcessingComplete QueryId
    | UnspecifiedMutationInQuery QueryId EventRecord
    | MutatePerson QueryId EventRecord
    | MutateAddress QueryId EventRecord
    | EventProcessingError ( String, String )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    let
        updateEngine : Engine.Msg -> Model -> ( Model, Cmd Msg )
        updateEngine =
            ParentChildUpdate.updateChildApp (Engine.update engineConfig) update .engineModel engineConfig.routeToMeTagger (\model engineModel -> { model | engineModel = engineModel })

        engineDelete : QueryId -> ( ( ( Model, Cmd Msg ), List msg ), Maybe CascadingDelete ) -> ( Model, Cmd Msg )
        engineDelete queryId ( ( ( model, cmd ), _ ), maybeCascadingDelete ) =
            let
                newEngineModel =
                    maybeCascadingDelete
                        |?> Engine.cascadingDeleteOccurred model.engineModel queryId
                        ?= model.engineModel
            in
                { model | engineModel = newEngineModel } ! [ cmd ]

        processCascadingMutationResult =
            Mutation.processCascadingMutationResult model
                deleteTaggers
                (\msg model -> ( update msg model, [] ))

        processMutationResult =
            Mutation.processMutationResult model

        crash error =
            Debug.crash ("Program Bug:" +-+ error)
    in
        case msg of
            Nop ->
                model ! []

            MutatePerson queryId eventRecord ->
                let
                    l =
                        Debug.log "MutatePerson" eventRecord

                    -- event =
                    --     eventRecord.event
                in
                    handleMutationCascadingDelete Person.defaultFragment Person.mutate model.personFragments eventRecord
                        |> processCascadingMutationResult
                            queryId
                            eventRecord
                            (\model newDict -> { model | personFragments = Debug.log "New Person Dictionary" newDict })
                            (mutationError "Person")
                        |> engineDelete queryId

            -- -- an example of how to remove entities from the dictionary when they lose a property (or any criteria for that matter)
            -- -- N.B. the use of ?! so we don't do unnecessary dictionary manipulation every time
            -- (( getOperation event ??= crash, getPropertyName event ??= (\_ -> "") ) == ( "removed", "address" ))
            --     ?! ( (\_ -> { model | personFragments = Debug.log "New Person Dictionary AFTER DELETE" <| Dict.remove (getEntityId event ??= crash) model.personFragments } ! [])
            --        , (\_ ->
            --             handleMutationCascadingDelete personFragmentshell Person.mutate model.personFragments eventRecord.event
            --                 |> processCascadingMutationResult
            --                     queryId
            --                     eventRecord
            --                     (\model newDict -> { model | personFragments = Debug.log "New Person Dictionary" newDict })
            --                     (mutationError "Person")
            --                 |> engineDelete queryId
            --          )
            --        )
            MutateAddress queryId eventRecord ->
                let
                    l =
                        Debug.log "MutateAddress" eventRecord
                in
                    handleMutation Address.defaultFragment Address.mutate model.addressFragments eventRecord
                        |> processMutationResult
                            (\model newDict -> { model | addressFragments = Debug.log "New Address Dictionary" newDict })
                            (mutationError "Address")
                        |> first

            EngineLog ( level, ( queryId, err ) ) ->
                let
                    l =
                        Debug.log "EngineLog" ( level, ( queryId, err ) )
                in
                    model ! []

            EngineError ( type_, ( queryId, err ) ) ->
                let
                    l =
                        Debug.log "EngineError" ( type_, ( queryId, err ) )
                in
                    model ! []

            EngineModule engineMsg ->
                updateEngine engineMsg model

            EventProcessingComplete queryId ->
                let
                    l =
                        Debug.log "EventProcessingComplete" ""

                    projectionResult =
                        Dict.get queryId model.queries
                            |?> (\projection -> projection <| WrappedModel model)
                            ?= (Err [ "Unknown query id:" +-+ queryId ])

                    crashIfBadProjection =
                        projectionResult ??= (Debug.crash << String.join "\n")

                    newModel =
                        projectionResult |??> (\wrappedModel -> unwrapModel wrappedModel) ??= (\_ -> model)

                    ( newEngineModel, cmd ) =
                        newModel.didRefresh
                            ? ( refreshQuery newModel.engineModel queryId
                                    |??> identity
                                    ??= (\error -> Debug.crash ("refreshQuery:" +-+ error))
                              , -- let
                                --         json =
                                --             Debug.log "json" <| Engine.exportQueryState newModel.engineModel queryId |??> identity ??= (\error -> Debug.crash "exportQueryState:" +-+ error)
                                --
                                --         result =
                                --             Debug.log "import" <| Engine.importQueryState personQuery newModel.engineModel json
                                --     in
                                ( newModel.engineModel, Cmd.none )
                              )
                in
                    { newModel | didRefresh = True, engineModel = newEngineModel } ! [ cmd ]

            EventError eventRecord ( queryId, err ) ->
                let
                    l =
                        Debug.crash <| "Event Processing error:" +-+ err +-+ "for:" +-+ eventRecord +-+ "on query:" +-+ queryId
                in
                    model ! []

            UnspecifiedMutationInQuery queryId eventRecord ->
                let
                    l =
                        Debug.crash <| "Bad query, missing mutation message for: " +-+ queryId +-+ "eventRecord:" +-+ eventRecord
                in
                    model ! []

            EventProcessingError ( eventStr, error ) ->
                let
                    l =
                        Debug.crash <| "Event Processing Error:" +-+ error +-+ "\nEvent:" +-+ eventStr
                in
                    model ! []


unwrapModel : WrappedModel -> Model
unwrapModel wrappedModel =
    case wrappedModel of
        WrappedModel model ->
            model


projectPerson : WrappedModel -> Result ProjectionErrors WrappedModel
projectPerson wrappedModel =
    let
        model =
            unwrapModel wrappedModel

        addressProjections =
            projectMap toAddress model.addressFragments

        personProjections =
            projectMap (toPerson <| successfulProjections addressProjections) model.personFragments

        newModel =
            { model | persons = successfulProjections personProjections, addresses = successfulProjections addressProjections }

        allErrors =
            allFailedProjections [ failedProjections addressProjections, failedProjections personProjections ]
    in
        (allErrors == []) ? ( Ok <| WrappedModel newModel, Err allErrors )


defaultAddress : Address
defaultAddress =
    { street = Address.default |> .street
    }


{-|

    Convert address fragment to address
-}
toAddress : Address.Fragment -> Result ProjectionErrors Address
toAddress addressFragment =
    projectEntity
        []
        -- [ ( isNothing addressFragment.street, "street is missing" )
        -- ]
        { street = addressFragment.street ?= defaultAddress.street
        }


defaultPerson : Person
defaultPerson =
    { name = Person.default |> .name
    , address = defaultAddress
    , aliases = Person.default |> .aliases
    , oldAddresses = Person.default |> .oldAddresses
    }


{-| convert person fragment to person
-}
toPerson : AddressDict -> Person.Fragment -> Result ProjectionErrors Person
toPerson addresses personFragment =
    let
        getPerson address =
            projectEntity
                [ ( isNothing personFragment.name, "name is missing" )
                ]
                { name = personFragment.name ?= defaultPerson.name
                , address = address
                , aliases = personFragment.aliases.items
                , oldAddresses = personFragment.oldAddresses.items
                }

        address =
            personFragment.address
                |?> (\addressRef -> Dict.get addressRef addresses ?= defaultAddress)
                ?= defaultAddress
    in
        getPerson address


query : NodeQuery Msg
query =
    Query.mtQuery UnspecifiedMutationInQuery


personQuery : Query Msg
personQuery =
    -- Node { query | schema = Person.schema, properties = Just [ "name", "aliases", "oldAddresses" ], tagger = MutatePerson }
    Node { query | schema = Person.schema, tagger = MutatePerson }
        [ Leaf { query | schema = Address.schema, properties = Just [ "street" ], tagger = MutateAddress } ]


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none
