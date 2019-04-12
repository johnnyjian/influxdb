import {AppState} from 'src/shared/reducers/app'
import {VariablesState} from 'src/variables/reducers'
import {UserSettingsState} from 'src/userSettings/reducers'
import {OrgsState} from 'src/organizations/reducers/orgs'
import {AutoRefreshState} from 'src/shared/reducers/autorefresh'

export interface LocalStorage {
  VERSION: string
  app: AppState
  ranges: any[]
  autoRefresh: AutoRefreshState
  variables: VariablesState
  userSettings: UserSettingsState
  orgs: OrgsState
}
