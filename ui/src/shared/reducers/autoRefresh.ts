// Libraries
import {produce} from 'immer'

// Constants
import {
  AUTOREFRESH_DEFAULT_INTERVAL,
  AUTOREFRESH_DEFAULT_STATUS,
} from 'src/shared/constants'

// Types
import {Action, AutoRefreshStatus} from 'src/shared/actions/autorefresh'

export interface AutoRefreshState {
  [dashboardID: string]: {
    status: AutoRefreshStatus,
    interval: number
  }
}

const defaultAutoRefresh = () => ({
  status: AUTOREFRESH_DEFAULT_STATUS,
  interval: AUTOREFRESH_DEFAULT_INTERVAL
})

const initialState = (): AutoRefreshState => {
  return {}
}

export const autoRefreshReducer = (state = initialState(), action: Action) =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_AUTO_REFRESH_INTERVAL': {
        const {dashboardID, milliseconds} = action.payload

        if (!draftState[dashboardID]) {
          draftState[dashboardID] = defaultAutoRefresh()
        }

        if (milliseconds === 0) {
          draftState[dashboardID].status = AutoRefreshStatus.Paused
        } else {
          draftState[dashboardID].status = AutoRefreshStatus.Active
        }

        draftState[dashboardID].interval = milliseconds

        return
      }

      case 'SET_AUTO_REFRESH_STATUS': {
        const {dashboardID, status} = action.payload

        if (!draftState[dashboardID]) {
          draftState[dashboardID] = defaultAutoRefresh()
        }

        draftState[dashboardID].status = status

        return
      }
    }
  })
