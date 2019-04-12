// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'
import {connect} from 'react-redux'
import { withRouter, WithRouterProps } from 'react-router';

// Components
import {Dropdown} from 'src/clockface'
import {
  Button,
  ButtonShape,
  IconFont,
  ComponentStatus,
} from '@influxdata/clockface'

// Constants
import autoRefreshOptions, {
  AutoRefreshOption,
  AutoRefreshOptionType,
} from 'src/shared/data/autoRefreshes'

// Actions
import {
  setAutoRefreshInterval,
  AutoRefreshStatus,
} from 'src/shared/actions/autoRefresh'

// Types
import {AppState} from 'src/types'

const DROPDOWN_WIDTH_COLLAPSED = 50
const DROPDOWN_WIDTH_FULL = 84

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  showManualRefresh: boolean
  onManualRefresh?: () => void
}

interface StateProps {
  autoRefresh: number
  status: AutoRefreshStatus
}

interface DispatchProps {
  setAutoRefreshInterval: typeof setAutoRefreshInterval
}

type Props = DispatchProps & OwnProps & StateProps & WithRouterProps

@ErrorHandling
class AutoRefreshDropdown extends Component<Props> {
  public static defaultProps = {
    showManualRefresh: true
  }

  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
    }
  }

  public render() {
    return (
      <div className={this.className}>
        <Dropdown
          icon={this.dropdownIcon}
          widthPixels={this.dropdownWidthPixels}
          menuWidthPixels={DROPDOWN_WIDTH_FULL}
          onChange={this.handleDropdownChange}
          selectedID={this.selectedID}
          status={this.dropdownStatus}
        >
          {autoRefreshOptions.map(option => {
            if (option.type === AutoRefreshOptionType.Header) {
              return (
                <Dropdown.Divider
                  key={option.id}
                  id={option.id}
                  text={option.label}
                />
              )
            }

            return (
              <Dropdown.Item key={option.id} id={option.id} value={option}>
                {option.label}
              </Dropdown.Item>
            )
          })}
        </Dropdown>
        {this.manualRefreshButton}
      </div>
    )
  }

  public handleDropdownChange = (
    autoRefreshOption: AutoRefreshOption
  ): void => {
    const {params: {dashboardID}} = this.props
    const {milliseconds, } = autoRefreshOption

    this.props.setAutoRefreshInterval(dashboardID, milliseconds)
  }

  private get dropdownStatus(): ComponentStatus {
    if (this.isDisabled) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
  }

  private get isDisabled(): boolean {
    const {status} = this.props

    return status === AutoRefreshStatus.Disabled
  }

  private get isPaused(): boolean {
    const {status} = this.props

    return status === AutoRefreshStatus.Paused || this.isDisabled
  }

  private get className(): string {
    return classnames('autorefresh-dropdown', {paused: this.isPaused})
  }

  private get dropdownIcon(): IconFont {
    if (this.isPaused) {
      return IconFont.Pause
    }

    return IconFont.Refresh
  }

  private get dropdownWidthPixels(): number {
    if (this.isPaused) {
      return DROPDOWN_WIDTH_COLLAPSED
    }

    return DROPDOWN_WIDTH_FULL
  }

  private get selectedID(): string {
    const {autoRefresh} = this.props
    const selectedOption = autoRefreshOptions.find(
      option => option.milliseconds === autoRefresh
    )

    return selectedOption.id
  }

  private get manualRefreshButton(): JSX.Element {
    const {showManualRefresh, onManualRefresh} = this.props

    if (!showManualRefresh) {
      return
    }

    if (this.isPaused) {
      return (
        <Button
          shape={ButtonShape.Square}
          icon={IconFont.Refresh}
          onClick={onManualRefresh}
          customClass="autorefresh-dropdown--pause"
        />
      )
    }

    return null
  }
}

const mstp = ({autoRefresh}: AppState, {params: {dashboardID}}: Props): StateProps => {
  const {interval, status} = autoRefresh[dashboardID]

  return {autoRefresh: interval, status}
}

const mdtp = {
  setAutoRefreshInterval: setAutoRefreshInterval,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(AutoRefreshDropdown))
